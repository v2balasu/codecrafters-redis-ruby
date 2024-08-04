require_relative 'resp_data'
require 'securerandom'

class InvalidCommandError < StandardError; end

class CommandProcessor
  VALID_COMMANDS = %w[
    COMMAND
    ECHO
    PING
    SET
    GET
    INFO
    REPLCONF
    PSYNC
    WAIT
    CONFIG
    KEYS
    INCR
  ].freeze

  VALID_REPLICA_COMMANDS = %w[
    SET
    GET
    INFO
    PING
    REPLCONF
  ].freeze

  VALID_CONFIG_KEYS = %w[dir dbfilename].freeze

  def initialize(data_store:, repl_manager:)
    @data_store = data_store
    @repl_manager = repl_manager
    @client_id = SecureRandom.uuid
  end

  def execute(command:, args:)
    return nil if @repl_manager.role == 'slave' && !VALID_REPLICA_COMMANDS.include?(command.upcase)

    if VALID_COMMANDS.include?(command.upcase)
      result = send(command.downcase.to_sym, args)
      @repl_manager.increment_replica_offset(command: command, args: args)
      return result
    end

    raise InvalidCommandError, "#{command} is not a valid command"
  end

  private

  def command(_args)
    RESPData.new(type: :simple, value: 'OK')
  end

  def echo(args)
    RESPData.new(type: :bulk, value: args.first)
  end

  def ping(_args)
    @repl_manager.role == 'slave' ? nil : RESPData.new(type: :simple, value: 'PONG')
  end

  def info(_args)
    RESPData.new(type: :bulk, value: @repl_manager.serialize)
  end

  def replconf(args)
    if @repl_manager.role == 'slave' && (args&.first == 'GETACK' && args&.last == '*')
      return RESPData.new(type: :array, value: ['REPLCONF', 'ACK', @repl_manager.replica_offset.to_s])
    end

    RESPData.new(type: :simple, value: 'OK')
  end

  def psync(args)
    raise InvalidCommandError, 'Node is not a master' unless @repl_manager.role == 'master'

    req_repl_id, req_repl_offset = args
    raise InvalidCommandError unless req_repl_id == '?' && req_repl_offset == '-1'

    full_resync_resp = "FULLRESYNC #{@repl_manager.master_replid} #{@repl_manager.master_repl_offset}"
    RESPData.new(type: :simple, value: full_resync_resp)
  end

  def wait(args)
    num_replicas, timeout = args.map(&:to_i)
    sleep_seconds = timeout.to_f / 1000.00
    expiry = Time.now + sleep_seconds

    return RESPData.new(type: :integer, value: 0) if @repl_manager.replica_count == 0

    @repl_manager.ack_replicas(client_id: @client_id)
    count = @repl_manager.replicas_acked(client_id: @client_id)

    while (count.nil? || count < num_replicas) && (expiry.nil? || Time.now < expiry)
      sleep(0.1)
      @repl_manager.ack_replicas(client_id: @client_id) if count.nil?
      count = @repl_manager.replicas_acked(client_id: @client_id)
    end

    count = @repl_manager.replica_count if count.nil?
    RESPData.new(type: :integer, value: count)
  end

  def config(args)
    option, param = args
    raise InvalidCommandError, 'INVALID CONFIG OPTION' unless option&.upcase == 'GET'
    raise InvalidCommandError, 'INVALID CONFIG KEY' unless VALID_CONFIG_KEYS.include?(param&.downcase)

    data = if param.downcase == 'dir'
             ['dir', @data_store.rdb_dir]
           else
             ['dbfilename', @data_store.rdb_fname]
           end

    RESPData.new(type: :array, value: data)
  end

  def keys(args)
    raise InvalidCommandError, 'only * is supported' unless args&.first == '*'

    RESPData.new(type: :array, value: @data_store.keys)
  end

  def incr(args)
    key = args.first
    raise 'Key must be provided' if key.nil?

    value = @data_store.get(key)

    if value.nil?
      @data_store.set(key, '1', nil)
      return RESPData.new(type: :integer, value: 1)
    end

    raise 'Value is not numerical' unless value.to_i.to_s == value

    new_val = value.to_i + 1
    @data_store.update(key, new_val.to_s)
    RESPData.new(type: :integer, value: new_val)
  end

  def set(args)
    key, value, *expiry = args

    expiry_seconds = parse_expiry_seconds(expiry) unless expiry.empty?

    @repl_manager.queue_command('SET', args, @client_id) if @repl_manager.role == 'master'

    @data_store.set(key, value, expiry_seconds)

    @repl_manager.role == 'slave' ? nil : RESPData.new(type: :simple, value: 'OK')
  end

  def parse_expiry_seconds(expiry)
    unit_type = expiry.first.upcase
    raise InvalidCommandError, 'Invalid Expiry' unless %w[EX PX].include?(unit_type)

    denom = unit_type == 'PX' ? 1000.0 : 1
    expiry.last.to_i / denom
  end

  def get(args)
    val = @data_store.get(args.first)
    RESPData.new(type: :bulk, value: val)
  end
end
