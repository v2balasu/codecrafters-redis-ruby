require_relative 'message_parser'
require_relative 'command_processor'
require_relative 'response_encoder'

class ClientConnection
  include ResponseEncoder

  def initialize(socket:, data_store:)
    @socket = socket
    @data_store = data_store
  end

  def start
    loop do
      message = MessageParser.parse_message(socket: @socket)

      next unless message.is_a?(Array)

      command, *args = message

      begin
        response = CommandProcessor.execute(command: command, args: args, data_store: @data_store)
      rescue InvalidCommandError => e
        response = encode_response(type: :error, value: e.message)
      end

      response.split("\n").each { |w| @socket.puts w }
    end
  end
end
