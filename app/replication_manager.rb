require 'securerandom'
require_relative 'message_parser'
require_relative 'resp_data'

class ReplicationManager
  def self.complete_master_handshake(socket:, port:)
    ping_resp = send_handshake_command(socket: socket, data: ['PING'])
    raise 'Invalid Response' unless ping_resp == 'PONG'

    repl_resp = send_handshake_command(socket: socket, data: ['REPLCONF', 'listening-port', port.to_s])
    raise 'Invalid Response' unless repl_resp == 'OK'

    repl_resp = send_handshake_command(socket: socket, data: %w[REPLCONF capa psync2])
    raise 'Invalid Response' unless repl_resp == 'OK'

    psync_resp = send_handshake_command(socket: socket, data: %w[PSYNC ? -1])
    raise 'Invalid Response' unless psync_resp.match(/FULLRESYNC [A-z0-9]+ [0-9]+/)

    rdp_length = socket.gets&.chomp&.[](1..).to_i
    socket.read(rdp_length)
  end

  def self.send_handshake_command(socket:, data:)
    send_handshake_command = RESPData.new(type: :array, value: data).encode
    send_handshake_command.split('\r\n').each { |chunk| socket.puts chunk }
    MessageParser.parse_message(socket: socket)
  end
  
  attr_reader :role, :master_replid, :master_repl_offset, :master_handshake_complete, :replica_offset

  def initialize(role)
    @mutex = Thread::Mutex.new
    @replica_connections = []
    @replica_commands = []
    @role = role
    @master_replid = SecureRandom.alphanumeric(40) if @role == 'master'
    @master_repl_offset = 0 if @role == 'master'
    @replica_offset = 0
  end

  def serialize
    {
      role: @role,
      master_replid: @master_replid,
      master_repl_offset: @master_repl_offset
    }.compact
      .map { |k, v| "#{k}:#{v}\n" }.join
  end

  def increment_replica_offset(command:, args:)
    return unless @role == 'slave'

    bytes_processed = RESPData.new(type: :array, value: [command, args].flatten)
      .encode
      .bytes
      .length 
    
    @replica_offset += bytes_processed
  end

  def add_connection(socket:, data_store:)
    rdb_content = data_store.to_rdb.bytes
    socket.puts "$#{rdb_content.length}\r"
    socket.write rdb_content.pack('C*')
    @mutex.synchronize { @replica_connections << socket }
  end

  def replica_count
    @mutex.synchronize { @replica_connections.count }
  end

  def queue_command(command, args)
    @mutex.synchronize do
      return unless @replica_connections.length > 0

      resp_command = RESPData.new(type: :array, value: [command, args].flatten)
      @replica_commands << resp_command.encode
    end
  end

  def broadcast
    return unless @replica_connections.length > 0 && @replica_commands.length > 0

    @mutex.synchronize do
      unhealthy_connections = []

      while command = @replica_commands.shift
        @replica_connections.each do |connection|
          command.split('\n').each { |chunk| connection.puts chunk }
        rescue StandardError 
          unhealthy_connections << connection
        end
      end

      unhealthy_connections.each { |c| @replica_connections.remove(c) }
    end
  end
end
