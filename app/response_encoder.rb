module ResponseEncoder
  # TODO: move to other class and properly encode complex types
  def encode_response(type:, value:)
    return "$-1\r\n" if value.nil?

    return "-#{value}\r\n" if type == :error

    return "+#{value}\r\n" if type == :simple

    "$#{value.length}\r\n#{value}\r\n"
  end
end
