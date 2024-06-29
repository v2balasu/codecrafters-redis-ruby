require_relative 'message_parser'
require_relative 'command_processor'
require_relative 'response_encoder'

class ClientConnection
  include ResponseEncoder

  def initialize(socket:, command_processor:)
    @socket = socket
    @command_processor = command_processor
  end

  def start
    loop do
      message = MessageParser.parse_message(socket: @socket)

      next unless message.is_a?(Array)

      command, *args = message

      begin
        response = @command_processor.execute(command: command, args: args)
      rescue InvalidCommandError => e
        response = encode_response(type: :error, value: e.message)
      end

      response.split("\n").each { |w| @socket.puts w }
    end
  end
end
