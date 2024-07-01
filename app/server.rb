require 'optparse'
require 'socket'
require_relative './client_connection'
require_relative './data_store'
require_relative './resp_data'
require_relative './replication_manager'

class YourRedisServer # rubocop:disable Style/Documentation
  def initialize(port, master_host, master_port)
    @port = port
    @data_store = DataStore.new
    @master_host = master_host
    @master_port = master_port

    role = master_host.nil? ? 'master' : 'slave'
    @repl_manager = ReplicationManager.new(role)
  end

  def start
    master_handshake unless @repl_manager.role == 'master'
    client_listener = Thread.new { process_client_connections }
    Thread.new { broadcast_to_replicas }
    client_listener.join
  end

  private

  def server
    @server ||= TCPServer.new(@port)
  end

  def process_client_connections
    loop do
      socket = server.accept
      next unless socket

      create_connection(socket: socket)
    end
  end

  def broadcast_to_replicas
    loop do
      @repl_manager.broadcast
      sleep(0.1)
    end
  end

  def create_connection(socket:)
    # TODO: Pooling and state management
    Thread.new do
      connection = ClientConnection.new(
        socket: socket,
        command_processor: CommandProcessor.new(data_store: @data_store, repl_manager: @repl_manager)
      )
      status = connection.start

      if status == :upgrade_to_replica
        pp 'Upgrading connection to replica'
        rdb_content = @data_store.to_rdb.bytes
        socket.puts "$#{rdb_content.length}\r"
        socket.write rdb_content.pack('C*')

        @repl_manager.add_connection(socket: socket)
      end
    end
  end

  def master_handshake
    socket = TCPSocket.new(@master_host, @master_port)

    ping_resp = send_command(socket: socket, data: ['PING'])
    raise 'Invalid Response' unless ping_resp == 'PONG'

    repl_resp = send_command(socket: socket, data: ['REPLCONF', 'listening-port', @port.to_s])
    raise 'Invalid Response' unless repl_resp == 'OK'

    repl_resp = send_command(socket: socket, data: %w[REPLCONF capa psync2])
    raise 'Invalid Response' unless repl_resp == 'OK'

    psync_resp = send_command(socket: socket, data: %w[PSYNC ? -1])
    raise 'Invalid Response' unless psync_resp.match(/FULLRESYNC [A-z0-9]+ [0-9]+/)
  end

  def send_command(socket:, data:)
    ping_command = RESPData.new(type: :array, value: data).encode
    ping_command.split('\r\n').each { |chunk| socket.puts chunk }
    MessageParser.parse_message(socket: socket)
  end
end

options = {}
OptionParser.new do |parser|
  parser.banner = 'Usage: server.rb [options]'
  parser.on('--port PORT', Integer, 'Specify the port number') { |p| options[:port] = p }
  parser.on('--replicaof <MASTER_HOST>_<MASTER_PORT>', String, 'Specify the port number') do |address|
    options[:master_address] = address
  end
end.parse!

master_address = options[:master_address]
if !master_address.nil? && !master_address.match(/[A-z0-9]+\s[0-9]+/)
  pp 'Invalid replication address'
  exit
end
master_host, master_port = master_address.split(' ') unless master_address.nil?

port = options[:port] || 6379
YourRedisServer.new(port, master_host, master_port).start
