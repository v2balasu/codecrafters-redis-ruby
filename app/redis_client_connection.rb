class RedisClientConnection
  def initialize(socket:)
    @socket = socket
  end

  def start
    loop do 
      msg = @socket.gets
      return unless msg
    
      @socket.puts "+OK\r\n" if msg.start_with?('COMMAND')
      @socket.puts "+PONG\r\n" if msg.include?('PING')
    end
  end
end
