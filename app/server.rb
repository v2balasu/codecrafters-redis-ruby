require 'socket'
require_relative './redis_client_connection'

class YourRedisServer # rubocop:disable Style/Documentation
  def initialize(port)
    @port = port
  end

  def server
    @server ||= TCPServer.new(@port)
  end

  def start
    loop do
      socket = server.accept
      next unless socket 

      connection = RedisClientConnection.new(socket: socket)
      Thread.new { connection.start }
    end
  end
end

YourRedisServer.new(6379).start
