module RESPEncoder
  def encode(type:, value:)
    return "$-1\r\n" if value.nil?

    return "-#{value}\r\n" if type == :error

    return "+#{value}\r\n" if type == :simple

    return "$#{value.length}\r\n#{value}\r\n" if type == :bulk

    if type == :array && value.is_a?(Array)
      inner_content = value.map do |element|
        element_type = element.is_a?(Array) ? :array : :bulk
        encode(type: element_type, value: element)
      end.join

      return "*#{value.length}\r\n#{inner_content}"
    end

    raise 'Not a valid input'
  end
end
