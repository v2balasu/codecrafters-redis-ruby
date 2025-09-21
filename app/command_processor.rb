require_relative 'resp_data'
require 'securerandom'

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
    LRANGE
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

    entry_id = @data_store.with_stream_lock(stream_key) do |stream, current_entry_id|
      id = if raw_entry_id != '*'
             extract_entry_id(raw_entry_id, current_entry_id)
           else
             "#{(Time.now.to_f * 1000).to_i}-0"
           end

      entry = { id: id }

      while entry_kv.length > 0
        key, value = entry_kv.shift(2)
        raise InvalidCommandError, 'Missing value for stream entry' if value.nil?

        entry[key] = value
      end

      stream << entry
    end

    RESPData.new(entry_id)
  end

  def extract_entry_id(raw_entry_id, current_entry_id)
    current_ms, current_seq_no = current_entry_id&.split('-')&.map(&:to_i)
    new_ms_and_seq_no = /(?<ms>[0-9]+)-(?<seq_no>\*|[0-9]+)/.match(raw_entry_id)

    if new_ms_and_seq_no
      new_ms = new_ms_and_seq_no['ms'].to_i
      new_seq_no = if new_ms_and_seq_no['seq_no'] == '*'
                     if current_seq_no.nil? || current_ms != new_ms
                       new_ms == 0 ? 1 : 0
                     else
                       current_seq_no + 1
                     end
                   else
                     new_ms_and_seq_no['seq_no'].to_i
                   end
    end

    if new_ms.nil? || new_seq_no.nil? || (new_ms.zero? && new_seq_no.zero?)
      raise InvalidCommandError,
            'The ID specified in XADD must be greater than 0-0'
    end

    entry_id = "#{new_ms}-#{new_seq_no}"

    return entry_id if current_ms.nil? || new_ms > current_ms

    return entry_id if new_ms == current_ms && (current_seq_no.nil? || new_seq_no > current_seq_no)

    raise InvalidCommandError, 'The ID specified in XADD is equal or smaller than the target stream top item'
  end

  def type(args)
    key = args.first

    raise InvalidCommandError, 'key must be provided' if key.nil?

    val = @data_store.get(key)
    resp_type = if val.nil?
                  'none'
                elsif val.is_a?(Array)
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

    range = @data_store.get(stream_key)

    raise InvalidCommandError, 'Stream not found' if range.nil? || !range.is_a?(Array)

    range = range.reject { |entry| entry[:id] < start_id } unless start_id.nil? || start_id == '-'
    range = range.reject { |entry| entry[:id] > end_id } unless end_id.nil? || end_id == '+'

    data = range.map { |entry| [entry[:id], entry.reject { |k| k == :id }.to_a.flatten] }

    RESPData.new(data)
  end

  def xread(args)
    option = args.shift
    block_ms = args.shift if option.upcase == 'BLOCK'
    block_until_read = block_ms&.to_i&.zero?
    block_until_time = (Time.now + block_ms.to_i / 1000 if block_ms && !block_until_read)

    args.reject! { |a| a.upcase == 'STREAMS' }
    raise InvalidCommandError, 'Invalid arg count' unless args.length.even?

    stream_keys = args[0..args.length / 2 - 1]
    raw_search_ids = args[(args.length / 2)..]
    search_ids = []

    raw_search_ids.each_with_index do |id, idx|
      if id != '$'
        search_ids << id
        next
      end

      stream_key = stream_keys[idx]
      stream = @data_store.get(stream_key)

      if stream.nil?
        search_ids << '0-0'
        next
      end

      search_ids << stream.last[:id]
    end

    ranges = {}

    loop do
      stream_keys.each_with_index do |key, idx|
        search_id = search_ids[idx]
        stream = @data_store.get(key)
        range = stream&.reject { |entry| entry[:id] <= search_id }
        next unless range && !range.empty?

        ranges[key] = range
      end

      break if !ranges.empty? || (block_until_time && (Time.now > block_until_time))
    end

    return RESPData.new(RESPData::NullArray.new) if ranges.empty?

    data = ranges.map do |stream_key, range|
      [
        stream_key,
        range.map { |entry| [entry[:id], entry.reject { |k| k == :id }.to_a.flatten] }
      ]
    end

    RESPData.new(data)
  end

  def rpush(args)
    list_key, *values = args

    list = @data_store.get(list_key) || []
    list.concat(values)
    @data_store.set(list_key, list, nil)

    RESPData.new(list.length)
  end

  def lrange(args)
    list_key, *range = args
    start_idx, stop_idx = range.map(&:to_i)

    raise InvalidCommandError, 'Invalid Params Provided for LRANGE' unless list_key && start_idx && stop_idx

    list = @data_store.get(list_key)

    return RESPData.new([]) unless list

    start_idx = start_idx.negative? ? [0, start_idx + list.length].max : start_idx
    stop_idx = stop_idx.negative? ? [0, stop_idx + list.length].max : stop_idx

    start_idx = [start_idx, list.length - 1].min
    stop_idx = [stop_idx, list.length - 1].min

    return RESPData.new([]) if start_idx > stop_idx

    RESPData.new(list[start_idx..stop_idx])
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
