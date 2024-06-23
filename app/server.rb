require 'socket'

class YourRedisServer # rubocop:disable Style/Documentation
  def initialize(port)
    @port = port
  end

  def server
    @server ||= TCPServer.new(@port)
  end

  def start
    loop do
      client = server.accept
      client.puts "+PONG\r\n"
    end
  end
end

YourRedisServer.new(6379).start
