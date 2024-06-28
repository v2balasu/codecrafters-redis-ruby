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

    def command(_args, _data_store)
      'OK'
    end

    def echo(args, _data_store)
      args.first
    end

    def ping(_args, _data_store)
      'PONG'
    end

    def set(args, data_store)
      key, value = *args
      data_store.set(key, value) unless value.nil?
      'OK'
    end

    def get(args, data_store)
      val = data_store.get(args.first)
      val.nil? ? '(nil)' : val
    end
  end
end
