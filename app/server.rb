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
      msg = client.gets
      client.puts "+OK\r\n" if msg.start_with?('COMMAND')
      client.puts "+PONG\r\n" if msg.include?('PING')
    rescue StandardError
      client = server.accept
    end
  end
end

YourRedisServer.new(6379).start
