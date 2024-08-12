class RESPData
  attr_reader :value

  def initialize(value, string_type: nil)
    @value = value
    @string_type = string_type

    raise 'String Type modifier only allowed for string values' if !@value.is_a?(String) && @string_type
  end

  def encode
    encode_data(value: @value, string_type: @string_type)
  end

  private

  # TODO: use stack instead of recursion
  def encode_data(value:, string_type:)
    return "$-1\r\n" if value.nil?

    return ":#{value}\r\n" if value.is_a?(Integer)

    return "-#{value.message}\r\n" if value.is_a?(StandardError)

    return "+#{value}\r\n" if value.is_a?(String) && string_type == :simple

    return "$#{value.length}\r\n#{value}\r\n" if value.is_a?(String)

    return unless value.is_a?(Array)

    inner_content = value.map do |element|
      encode_data(value: element, string_type: nil)
    end.join

    "*#{value.length}\r\n#{inner_content}"
  end
end
