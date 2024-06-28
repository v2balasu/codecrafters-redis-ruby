require_relative 'message_parser'
require_relative 'command_processor'

class ClientConnection
  def initialize(socket:)
    @socket = socket
  end

  def start
    loop do
      message = MessageParser.parse_message(socket: @socket)

      next unless message.is_a?(Array)

      command, *args = message

      begin
        result = CommandProcessor.execute(command, args)
        response = "+#{result}\r\n"
      rescue InvalidCommandError => e
        response = "-#{e.message}\r\n"
      end

      @socket.puts response
    end
  end
end
