require 'socket'
require_relative './client_connection'
require_relative './data_store'

class YourRedisServer # rubocop:disable Style/Documentation
  def initialize(port)
    @port = port
    @data_store = DataStore.new
    @server_info = {
      role: 'master'
      # connected_slaves: 0,
      # master_replid: 0,
      # master_repl_offset: 0
    }
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

port = 6379
port = ARGV[1].to_i if ARGV.first == '--port' && !ARGV[1].nil?

YourRedisServer.new(port).start
