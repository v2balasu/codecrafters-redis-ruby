require 'optparse'
require 'socket'
require 'securerandom'
require_relative './client_connection'
require_relative './data_store'

class YourRedisServer # rubocop:disable Style/Documentation
  def initialize(port, master_address)
    @port = port
    @data_store = DataStore.new
    @server_info = {
      role: master_address.nil? ? 'master' : 'slave'
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

  def start
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

port = options[:port] || 6379
YourRedisServer.new(port, master_address).start
