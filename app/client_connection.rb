require_relative 'message_parser'
require_relative 'command_processor'
require_relative 'resp_data'

class ClientConnection
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
        response = encode(type: :error, value: e.message)
      end

      response.encode.split('\n').each { |chunk| @socket.puts(chunk) }

      return :upgrade_to_replica if command.upcase == 'PSYNC'
    end
  end
end
