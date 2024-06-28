require_relative 'response_encoder'
class InvalidCommandError < StandardError; end

class CommandProcessor
  extend ResponseEncoder

  VALID_COMMANDS = %w[
    COMMAND
    ECHO
    PING
    SET
    GET
  ]

  class << self
    def execute(command:, args:, data_store:)
      return send(command.downcase.to_sym, args, data_store) if VALID_COMMANDS.include?(command.upcase)

      raise InvalidCommandError, "#{command} is not a valid command"
    end

    private

    def command(_args, _data_store)
      encode_response(type: :simple, value: 'OK')
    end

    def echo(args, _data_store)
      encode_response(type: :bulk, value: args.first)
    end

    def ping(_args, _data_store)
      encode_response(type: :simple, value: 'PONG')
    end

    def set(args, data_store)
      key, value, *expiry = args

      expiry_seconds = parse_expiry_seconds(expiry) unless expiry.empty?

      data_store.set(key, value, expiry_seconds)
      encode_response(type: :simple, value: 'OK')
    end

    def parse_expiry_seconds(expiry)
      unit_type = expiry.first.upcase
      raise InvalidCommandError, 'Invalid Expiry' unless %w[EX PX].include?(unit_type)

      denom = unit_type == 'PX' ? 1000.0 : 1
      expiry.last.to_i / denom
    end

    def get(args, data_store)
      val = data_store.get(args.first)
      encode_response(type: :bulk, value: val)
    end
  end
end
