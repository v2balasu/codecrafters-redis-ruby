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

      if response.is_a?(Array)
        response.each { |r| transmit_response(r) }
      else
        transmit_response(response)
      end
    end
  end

  private

  def transmit_response(response)
    return transmit_resp_response(response.encode) if response.is_a?(RESPData)

    @socket.puts "$#{response.bytes.length}\r"
    @socket.write response.bytes.pack('C*')
  end

  def transmit_resp_response(response)
    response.split("\n").each { |w| @socket.puts w }
  end
end
