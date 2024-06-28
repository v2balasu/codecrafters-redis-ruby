class InvalidCommandError < StandardError; end

class CommandProcessor
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

    # TODO: move to other class and properly encode complex types
    def encode_response(type:, value:)
      return "$-1\r\n" if value.nil?

      return "+#{value}\r\n" if type == :simple

      "$#{value.length}\r\n#{value}\r\n"
    end

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

      unless expiry.empty?
        unit_type = expiry.first.upcase
        raise InvalidCommandError, 'Invalid Expiry' unless %w[EX PX].include?(unit_type)

        denom = unit_type == 'PX' ? 1000 : 1
        expiry_seconds = expiry.last.to_i / denom
      end

      data_store.set(key, value, expiry_seconds)
      encode_response(type: :simple, value: 'OK')
    end

    def get(args, data_store)
      val = data_store.get(args.first)
      encode_response(type: :bulk, value: val)
    end
  end
end
