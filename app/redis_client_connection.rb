require_relative 'message_parser'
class RedisClientConnection
  def initialize(socket:)
    @socket = socket
  end

  def start
    loop do
      message = MessageParser.parse_message(socket: @socket)

      next unless message.is_a?(Array)

      @socket.puts process_command(message)
    end
  end

  def process_command(message)
    command = message.first
    args = message[1..]

    case command.upcase
    when 'COMMAND'
      "+OK\r\n"
    when 'PING'
      "+PONG\r\n"
    when 'ECHO'
      "+ECHO #{args.first}\r\n"
    else
      "-Unsupported Command: #{command}\r\n"
    end
  end
end
