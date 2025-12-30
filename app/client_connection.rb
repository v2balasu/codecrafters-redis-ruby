require 'fcntl'
require_relative 'message_parser'
require_relative 'command_processor'
require_relative 'resp_data'

class ClientConnection
  attr_reader :socket

  def initialize(socket:, command_processor:)
    @socket = socket
    @command_processor = command_processor
    @parser = MessageParser.new
    @write_buffer = ''
    @closed = false

    # Set socket to non-blocking mode
    @socket.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
  end

  # Called by event loop when socket is readable
  def on_readable
    data = @socket.read_nonblock(4096)

    # Process the data through the parser
    result = @parser.process(data)

    # If we have a complete message, process it
    if result.status == :complete
      process_message(result.message)
    end
  rescue IO::WaitReadable
    # No data available right now, will be called again
  rescue EOFError, Errno::ECONNRESET
    close
  end

  # Called by event loop when socket is writable
  def on_writable
    return if @write_buffer.empty?

    bytes_written = @socket.write_nonblock(@write_buffer)
    @write_buffer = @write_buffer[bytes_written..-1] || ''
  rescue IO::WaitWritable
    # Socket not ready for writing, will be called again
  rescue EOFError, Errno::ECONNRESET, Errno::EPIPE
    close
  end

  # Check if connection wants to read
  def wants_read?
    !@closed
  end

  # Check if connection wants to write
  def wants_write?
    !@closed && !@write_buffer.empty?
  end

  # Check if connection is closed
  def closed?
    @closed
  end

  # Check if connection needs to be upgraded to replica
  def needs_replica_upgrade?
    @upgrade_to_replica || false
  end

  # Close the connection
  def close
    @closed = true
    @socket.close unless @socket.closed?
  end

  # Legacy blocking start method for backward compatibility
  def start
    loop do
      message = MessageParser.parse_message(socket: @socket)

      next unless message.is_a?(Array)

      command, *args = message

      begin
        response = @command_processor.execute(command: command, args: args)
      rescue InvalidCommandError => e
        response = RESPData.new(e)
      end

      response&.encode&.split('\n')&.each { |chunk| @socket.puts(chunk) }

      return :upgrade_to_replica if command.upcase == 'PSYNC'
    end
  end

  private

  def process_message(message)
    return unless message.is_a?(Array)

    command, *args = message

    begin
      response = @command_processor.execute(command: command, args: args)
    rescue InvalidCommandError => e
      response = RESPData.new(e)
    end

    # Queue response for writing
    if response
      encoded = response.encode
      @write_buffer << encoded
    end

    # Handle PSYNC upgrade (will need special handling in event loop)
    if command.upcase == 'PSYNC'
      @upgrade_to_replica = true
    end
  end
end
