require 'securerandom'

class ReplicationManager
  attr_reader :role, :master_replid, :master_repl_offset

  def initialize(role)
    @mutex = Thread::Mutex.new
    @replica_connections = []
    @replica_commands = []
    @role = role
    @master_replid = SecureRandom.alphanumeric(40) if @role == 'master'
    @master_repl_offset = 0 if @role == 'master'
  end

  def serialize
    {
      role: @role,
      master_replid: @master_replid,
      master_repl_offset: @master_repl_offset
    }.compact
      .map { |k, v| "#{k}:#{v}\n" }.join
  end

  def add_connection(socket:)
    pp 'Adding replica'
    @mutex.synchronize { @replica_connections << socket }
  end

  def queue_command(command, args)
    @mutex.synchronize do
      return unless @replica_connections.length > 0

      resp_command = RESPData.new(type: :array, value: [command, args].flatten)
      @replica_commands << resp_command.encode
    end
  end

  def broadcast
    return unless @replica_connections.length > 0 && @replica_commands.length > 0

    @mutex.synchronize do
      unhealthy_connections = []

      while command = @replica_commands.shift
        @replica_connections.each do |connection|
          command.split('\n').each { |chunk| connection.puts chunk }
        rescue StandardError => e
          pp "Unable to send to replica #{e.message}"
          unhealthy_connections << connection
        end
      end

      unhealthy_connections.each { |c| @replica_connections.remove(c) }
    end
  end
end
