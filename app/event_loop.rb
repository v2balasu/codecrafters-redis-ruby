require_relative 'client_connection'
require_relative 'command_processor'

class EventLoop
  def initialize(server_socket:, data_store:, repl_manager:)
    @server_socket = server_socket
    @connections = {}     # socket => ClientConnection instance
    @running = false
    @data_store = data_store
    @repl_manager = repl_manager
  end

  # Main event loop
  def run
    @running = true

    while @running
      # Clean up closed connections
      @connections.delete_if { |_, conn| conn.closed? }

      # Monitor server socket for new connections, plus all client sockets
      readable_sockets = [@server_socket]
      writable_sockets = []

      @connections.each do |socket, conn|
        # Don't read from blocked connections (they're waiting for stream data, not client input)
        readable_sockets << socket if conn.wants_read? && !conn.blocked?
        writable_sockets << socket if conn.wants_write?
      end

      # Use IO.select to wait for events
      readable, writable, _ = IO.select(
        readable_sockets,
        writable_sockets.empty? ? nil : writable_sockets,
        nil,
        0.1  # 100ms timeout
      )

      # Process readable sockets
      readable&.each do |socket|
        if socket == @server_socket
          accept_new_connection
        else
          process_readable(socket: socket)
        end
      end

      # Process writable sockets
      writable&.each do |socket|
        process_writable(socket: socket)
      end

      # Resume blocked connections (check if stream data is available)
      @connections.each do |_socket, conn|
        conn.resume if conn.blocked?
      end

      # Handle replica upgrades after writes are flushed
      @connections.each do |socket, conn|
        if conn.needs_replica_upgrade? && !conn.wants_write?
          handle_replica_upgrade(socket: socket, connection: conn)
        end
      end
    end
  end

  # Stop the event loop
  def stop
    @running = false
  end

  private

  def accept_new_connection
    client_socket = @server_socket.accept

    # Create ClientConnection with CommandProcessor
    command_processor = CommandProcessor.new(
      data_store: @data_store,
      repl_manager: @repl_manager
    )

    connection = ClientConnection.new(
      socket: client_socket,
      command_processor: command_processor
    )

    @connections[client_socket] = connection
  end

  def process_readable(socket:)
    connection = @connections[socket]
    connection&.on_readable
  end

  def process_writable(socket:)
    connection = @connections[socket]
    connection&.on_writable
  end

  def handle_replica_upgrade(socket:, connection:)
    # Add the connection to the replication manager
    @repl_manager.add_connection(socket: socket, data_store: @data_store)

    # Remove from event loop connections (replication manager takes over)
    @connections.delete(socket)
  end
end
