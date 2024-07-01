require_relative 'resp_encoder'
class InvalidCommandError < StandardError; end

class CommandProcessor
  include RESPEncoder

  VALID_COMMANDS = %w[
    COMMAND
    ECHO
    PING
    SET
    GET
    INFO
    REPLCONF
    PSYNC
  ]

  def initialize(data_store:, server_info:)
    @data_store = data_store
    @server_info = server_info
  end

  def execute(command:, args:)
    return send(command.downcase.to_sym, args) if VALID_COMMANDS.include?(command.upcase)

    raise InvalidCommandError, "#{command} is not a valid command"
  end

  private

  def command(_args)
    encode(type: :simple, value: 'OK')
  end

  def echo(args)
    encode(type: :bulk, value: args.first)
  end

  def ping(_args)
    encode(type: :simple, value: 'PONG')
  end

  def info(_args)
    str = @server_info.map { |k, v| "#{k}:#{v}\n" }.join
    encode(type: :bulk, value: str)
  end

  def replconf(_args)
    encode(type: :simple, value: 'OK')
  end

  def psync(args)
    raise InvalidCommandError, 'Node is not a master' unless @server_info[:role] == 'master'

    req_repl_id, req_repl_offset = args
    raise InvalidCommandError unless req_repl_id == '?' && req_repl_offset == '-1'

    full_resync_resp = "FULLRESYNC #{@server_info[:master_replid]} #{@server_info[:master_repl_offset]}"
    [
      encode(type: :simple, value: full_resync_resp),
      @data_store.to_rdb_bytes
    ]
  end

  def set(args)
    key, value, *expiry = args

    expiry_seconds = parse_expiry_seconds(expiry) unless expiry.empty?

    @data_store.set(key, value, expiry_seconds)
    encode(type: :simple, value: 'OK')
  end

  def parse_expiry_seconds(expiry)
    unit_type = expiry.first.upcase
    raise InvalidCommandError, 'Invalid Expiry' unless %w[EX PX].include?(unit_type)

    denom = unit_type == 'PX' ? 1000.0 : 1
    expiry.last.to_i / denom
  end

  def get(args)
    val = @data_store.get(args.first)
    encode(type: :bulk, value: val)
  end
end
