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
    def execute(command, args)
      return send(command.downcase.to_sym, args) if VALID_COMMANDS.include?(command.upcase)

      raise InvalidCommandError, "#{command} is not a valid command"
    end

    def command(args)
      'OK'
    end

    def echo(args)
      args.first
    end

    def ping(args)
      'PONG'
    end

    def set(args)
      'TEST'
    end

    def get(args)
      'TEST'
    end
  end
end
