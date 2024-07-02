class MessageParser
  AGGREGATE_KEYS = ['*', '$'].freeze

  class << self
    def parse_message(socket:)
      chunk = socket.gets&.chomp
      root = parse_chunk(chunk)
      return root unless root.is_a?(Aggregate)

      aggregate_stack = [root]

      while aggregate_stack.length > 0 && chunk = socket.gets&.chomp
        current = aggregate_stack.last

        value = if current.value.is_a?(Array)
                  parse_chunk(chunk)
                else
                  chunk
                end

        current.append_element(value)

        aggregate_stack.pop if current.complete?
        aggregate_stack.push(value) if value.is_a?(Aggregate)
      end

      return nil unless aggregate_stack.length == 0
      process_aggregate_values(root)
    end

    def parse_chunk(chunk)
      return nil unless chunk

      if AGGREGATE_KEYS.include?(chunk[0])
        parse_aggregate(chunk)
      else
        parse_simple_value(chunk)
      end
    end

    def parse_simple_value(chunk)
      return nil if chunk[0] == '_'

      value = chunk[1..]
      chunk[0] == ':' ? value.to_i : value
    end

    def parse_aggregate(chunk)
      value = chunk[0] == '*' ? [] : ''
      Aggregate.new(chunk[1..].to_i, value)
    end

    def process_aggregate_values(aggregate)
      return aggregate.value if aggregate.value.is_a?(String)

      aggregate.value.map { |v| process_aggregate_values(v) }
    end
  end

  class Aggregate
    attr_reader :value

    def initialize(max_length, value)
      @max_length = max_length
      @value = value
    end

    def complete?
      @value.length == @max_length
    end

    def append_element(element)
      if @value.is_a?(Array)
        @value.push(element)
      else
        @value.concat(element)
      end
    end
  end
end
