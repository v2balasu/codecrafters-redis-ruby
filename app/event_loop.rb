class EventLoop
  def initialize(server_socket:)
    @server_socket = server_socket
    @connections = {}     # socket => ClientConnection instance
    @running = false
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
        readable_sockets << socket if conn.wants_read?
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
    end
  end

  # Stop the event loop
  def stop
    @running = false
  end

  private

  def accept_new_connection
    client_socket = @server_socket.accept
    # TODO: Create ClientConnection and register it
    # This will be implemented when we integrate with the server
    # connection = ClientConnection.new(socket: client_socket, command_processor: ...)
    # @connections[client_socket] = connection
  end

  def process_readable(socket:)
    connection = @connections[socket]
    connection&.on_readable
  end

  def process_writable(socket:)
    connection = @connections[socket]
    connection&.on_writable
  end
end
