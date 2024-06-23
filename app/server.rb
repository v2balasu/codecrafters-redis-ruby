require 'socket'

class YourRedisServer # rubocop:disable Style/Documentation
  def initialize(port)
    @port = port
  end

  def server
    @server ||= TCPServer.new(@port)
  end

  def start
    client = server.accept

    loop do
      client.puts "+PONG\r\n"
    end
  end
end

YourRedisServer.new(6379).start
