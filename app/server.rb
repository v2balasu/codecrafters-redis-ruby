require 'optparse'
require 'socket'
require 'securerandom'
require_relative './client_connection'
require_relative './data_store'
require_relative './resp_encoder'

class YourRedisServer # rubocop:disable Style/Documentation
  include RESPEncoder

  def initialize(port, master_host, master_port)
    @port = port
    @data_store = DataStore.new

    if master_host && master_port
      @master_host = master_host
      @master_port = master_port
    end

    @server_info = {
      role: @master_host.nil? ? 'master' : 'slave'
    }

    return unless @server_info[:role] == 'master'

    @server_info.merge!({
                          master_replid: SecureRandom.alphanumeric(40),
                          master_repl_offset: 0
                        })
  end

  def server
    @server ||= TCPServer.new(@port)
  end

  def create_connection(socket:)
    # TODO: Pooling and state management
    Thread.new do
      connection = ClientConnection.new(
        socket: socket,
        command_processor: CommandProcessor.new(data_store: @data_store, server_info: @server_info)
      )
      connection.start
    end
  end

  def master_handshake
    socket = TCPSocket.new(@master_host, @master_port)

    # TODO: handle timeouts
    ping_response = send_to_master(socket: socket, data: ['PING'])

    raise 'Replica handshake failed on PING' unless ping_response == 'PONG'

    replica_listen_response = send_to_master(socket: socket, data: ['REPLCONF', 'listening-port', @port.to_s])

    raise 'Replica handshake failed on REPLCONF listening port' unless replica_listen_response == 'OK'

    replica_capa_respoonse = send_to_master(socket: socket, data: %w[REPLCONF capa psync2])

    raise 'Replica handhsake failed on REPLCONF capa' unless replica_capa_respoonse == 'OK'
  end

  def send_to_master(socket:, data:)
    ping_command = encode(type: :array, value: data)
    ping_command.split('\r\n').each { |chunk| socket.puts chunk }
    MessageParser.parse_message(socket: socket)
  end

  def start
    master_handshake unless @master_host.nil?

    loop do
      socket = server.accept
      next unless socket

      create_connection(socket: socket)
    end
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
