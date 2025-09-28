require_relative 'resp_data'
require_relative 'redis_stream'
require 'securerandom'
require 'timeout'

class InvalidCommandError < StandardError
  def message
    "ERR #{super}"
  end
end

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
    MULTI
    EXEC
    DISCARD
    TYPE
    XADD
    XRANGE
    XREAD
    RPUSH
    LPUSH
    LRANGE
    LLEN
    LPOP
  ].freeze

  VALID_REPLICA_COMMANDS = %w[
    SET
    GET
    INFO
    PING
    REPLCONF
  ].freeze

  VALID_CONFIG_KEYS = %w[dir dbfilename].freeze

  TRANSACTION_CLEARING_CMDS = %w[EXEC DISCARD]

  @@transaction_exec_mutex = Thread::Mutex.new

  def initialize(data_store:, repl_manager:)
    @data_store = data_store
    @repl_manager = repl_manager
    @client_id = SecureRandom.uuid
    @transaction_in_progress = false
    @queued_commands = []
  end

  def execute(command:, args:)
    return nil if @repl_manager.role == 'slave' && !VALID_REPLICA_COMMANDS.include?(command.upcase)

    if !TRANSACTION_CLEARING_CMDS.include?(command.upcase) && @transaction_in_progress
      @queued_commands << [command, args]
      return RESPData.new(RESPData::SimpleString.new('QUEUED'))
    end

    if VALID_COMMANDS.include?(command.upcase)
      result = send(command.downcase.to_sym, args)
      @repl_manager.increment_replica_offset(command: command, args: args)
      return result
    end

    raise InvalidCommandError, "#{command} is not a valid command"
  end

  private

  def command(_args)
    RESPData.new(RESPData::SimpleString.new('OK'))
  end

  def echo(args)
    RESPData.new(args.first)
  end

  def ping(_args)
    @repl_manager.role == 'slave' ? nil : RESPData.new(RESPData::SimpleString.new('PONG'))
  end

  def info(_args)
    RESPData.new(@repl_manager.serialize)
  end

  def replconf(args)
    if @repl_manager.role == 'slave' && args&.first == 'GETACK' && args&.last == '*'
      return RESPData.new(['REPLCONF', 'ACK', @repl_manager.replica_offset.to_s])
    end

    RESPData.new(RESPData::SimpleString.new('OK'))
  end

  def psync(args)
    raise InvalidCommandError, 'Node is not a master' unless @repl_manager.role == 'master'

    req_repl_id, req_repl_offset = args
    raise InvalidCommandError unless req_repl_id == '?' && req_repl_offset == '-1'

    full_resync_resp = "FULLRESYNC #{@repl_manager.master_replid} #{@repl_manager.master_repl_offset}"
    RESPData.new(RESPData::SimpleString.new(full_resync_resp))
  end

  def wait(args)
    num_replicas, timeout = args.map(&:to_i)
    sleep_seconds = timeout.to_f / 1000.00
    expiry = Time.now + sleep_seconds

    return RESPData.new(0) if @repl_manager.replica_count.zero?

    count = @repl_manager.ack_replicas(client_id: @client_id)

    while (count.nil? || count < num_replicas) && (expiry.nil? || Time.now < expiry)
      sleep(0.1)
      count = @repl_manager.ack_replicas(client_id: @client_id)
    end

    count = @repl_manager.replica_count if count.nil?
    @repl_manager.reset_replica_ack(client_id: @client_id)

    RESPData.new(count)
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

    RESPData.new(data)
  end

  def keys(args)
    raise InvalidCommandError, 'only * is supported' unless args&.first == '*'

    RESPData.new(@data_store.keys)
  end

  def incr(args)
    key = args.first
    raise InvalidCommandError, 'Key must be provided' if key.nil?

    value = @data_store.get(key)

    if value.nil?
      @data_store.set(key, '1', nil)
      return RESPData.new(1)
    end

    raise InvalidCommandError, 'value is not an integer or out of range' unless value.to_i.to_s == value

    new_val = value.to_i + 1
    @data_store.update(key, new_val.to_s)
    RESPData.new(new_val)
  end

  def multi(_args)
    @transaction_in_progress = true
    RESPData.new(RESPData::SimpleString.new('OK'))
  end

  def exec(_args)
    raise InvalidCommandError, 'EXEC without MULTI' unless @transaction_in_progress

    results = []

    @@transaction_exec_mutex.synchronize do
      while (command, args = @queued_commands.shift)
        begin
          resp = send(command.downcase.to_sym, args)
          results << resp.value
        rescue InvalidCommandError => e
          results << e
        end
      end
    end

    @transaction_in_progress = false

    RESPData.new(results)
  end

  def discard(_args)
    raise InvalidCommandError, 'DISCARD without MULTI' unless @transaction_in_progress

    @transaction_in_progress = false
    @queued_commands = []

    RESPData.new(RESPData::SimpleString.new('OK'))
  end

  def xadd(args)
    stream_key, raw_entry_id, *entry_kv = args
    key_values = {}

    while entry_kv.any?
      key, value = entry_kv.shift(2)
      raise InvalidCommandError, 'Missing value for stream entry' if value.nil?

      key_values[key] = value
    end

    stream = @data_store.get(stream_key) || RedisStream.new
    stream.add_entry(raw_entry_id, key_values)
    @data_store.set(stream_key, stream, nil)

    RESPData.new(stream.current_id)
  end

  def type(args)
    key = args.first

    raise InvalidCommandError, 'key must be provided' if key.nil?

    val = @data_store.get(key)
    resp_type = if val.nil?
                  'none'
                elsif val.is_a?(RedisStream)
                  'stream'
                else
                  'string'
                end

    RESPData.new(RESPData::SimpleString.new(resp_type))
  end

  def xrange(args)
    stream_key, start_id, end_id = args

    # TODO: start_id and end_id validation
    raise InvalidCommandError, 'Invalid inpput' if stream_key.nil?

    stream = @data_store.get(stream_key)

    raise InvalidCommandError, 'Stream not found' if stream.nil? || !stream.is_a?(RedisStream)

    range = stream.search_entries(start_id, end_id)
    RESPData.new(range.map(&:to_resp_array))
  end

  def xread(args)
    option = args.shift
    block_ms = (args.shift&.to_i if option.upcase == 'BLOCK')
    block_seconds = block_ms&.positive? ? block_ms.to_f / 1000.00 : nil

    args.reject! { |a| a.upcase == 'STREAMS' }

    raise InvalidCommandError, 'Invalid arg count' unless args.length.even?

    stream_key_to_id = parse_stream_id_lookup(args)
    ranges = {}

    begin
      Timeout.timeout(block_seconds) do
        loop do
          stream_key_to_id.each do |key, id|
            stream = @data_store.get(key)
            range = stream.search_after_id(id)
            ranges[key] = range if range.any?
          end

          break if ranges.any?
        end
      end
    rescue StandardError
      nil
    end

    stream_key_to_id.each do |key, id|
      stream = @data_store.get(key)
      range = stream.search_after_id(id)
      ranges[key] = range if range.any?
    end

    return RESPData.new(RESPData::NullArray.new) if ranges.empty?

    data = ranges.map do |stream_key, range|
      [
        stream_key,
        range.map(&:to_resp_array)
      ]
    end

    RESPData.new(data)
  end

  def parse_stream_id_lookup(args)
    stream_keys = args[0..args.length / 2 - 1]
    raw_search_ids = args[(args.length / 2)..]
    lookup = {}

    raw_search_ids.each_with_index do |id, idx|
      stream_key = stream_keys[idx]

      if id != '$'
        lookup[stream_key] = id
        next
      end

      stream = @data_store.get(stream_key)
      lookup[stream_key] = stream&.current_id || '0-0'
    end

    lookup
  end

  def rpush(args)
    list_key, *values = args

    list = @data_store.get(list_key) || []
    list.append(*values)
    @data_store.set(list_key, list, nil)

    RESPData.new(list.length)
  end

  def lpush(args)
    list_key, *values = args

    list = @data_store.get(list_key) || []
    list.prepend(*values.reverse)
    @data_store.set(list_key, list, nil)

    RESPData.new(list.length)
  end

  def lrange(args)
    list_key, *range = args

    raise InvalidCommandError, 'Invalid Params Provided for LRANGE' unless list_key && range&.length == 2

    list = @data_store.get(list_key)

    return RESPData.new([]) unless list

    start_idx, stop_idx = normalize_range(range, list.length)

    return RESPData.new([]) if start_idx > stop_idx

    RESPData.new(list[start_idx..stop_idx])
  end

  def llen(args)
    list_key = args.first
    list = @data_store.get(list_key)
    RESPData.new(list&.length || 0)
  end

  def lpop(args)
    list_key = args.first
    list = @data_store.get(list_key)
    RESPData.new(list&.shift)
  end

  def normalize_range(range, max_len)
    start_idx, stop_idx = range.map(&:to_i)

    start_idx = start_idx.negative? ? [0, start_idx + max_len].max : start_idx
    stop_idx = stop_idx.negative? ? [0, stop_idx + max_len].max : stop_idx

    start_idx = [start_idx, max_len].min
    stop_idx = [stop_idx, max_len].min

    [start_idx, stop_idx]
  end

  def set(args)
    key, value, *expiry = args

    expiry_seconds = parse_expiry_seconds(expiry) unless expiry.empty?

    @repl_manager.queue_command('SET', args, @client_id) if @repl_manager.role == 'master'

    @data_store.set(key, value, expiry_seconds)

    @repl_manager.role == 'slave' ? nil : RESPData.new(RESPData::SimpleString.new('OK'))
  end

  def parse_expiry_seconds(expiry)
    unit_type = expiry.first.upcase
    raise InvalidCommandError, 'Invalid Expiry' unless %w[EX PX].include?(unit_type)

    denom = unit_type == 'PX' ? 1000.0 : 1
    expiry.last.to_i / denom
  end

  def get(args)
    val = @data_store.get(args.first)
    RESPData.new(val)
  end
end
