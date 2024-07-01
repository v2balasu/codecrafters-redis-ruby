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
    @repl_manager.role == 'master' ? start_as_master : start_as_replica
  end

  private

  def start_as_master
    client_listener = Thread.new { process_client_connections }
    Thread.new { broadcast_to_replicas }
    client_listener.join
  end

  def start_as_replica
    master_socket = TCPSocket.new(@master_host, @master_port)
    @repl_manager.master_handshake(socket: master_socket, port: @port)
    master_connection = ClientConnection.new(socket: master_socket,
                                             command_processor: CommandProcessor.new(data_store: @data_store,
                                                                                     repl_manager: @repl_manager))
    master_listener = Thread.new { master_connection.start }
    connection_litener = Thread.new { process_client_connections }

    master_listener.join
    connection_litener.join
  end

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

      @repl_manager.add_connection(socket: socket, data_store: @data_store) if status == :upgrade_to_replica
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
