class RESPData
  VALID_TYPES = %i[nil simple bulk error array].freeze

  def initialize(type:, value:)
    @type = type
    @value = value

    raise 'Invalid type' unless VALID_TYPES.include?(@type)
    raise "Invalid Value for type #{type}" if @type == :simple && !@value.is_a?(String)
    raise 'Value must be nil for type nil' if @type == :nil && !@value.nil?
    raise 'Value must be array for type array' if @type == :array && !@value.is_a?(Array)
  end

  def encode
    encode_data(type: @type, value: @value)
  end

  private

  # TODO: use stack instead of recursion
  def encode_data(type:, value:)
    return "$-1\r\n" if value.nil?

    return "-#{value}\r\n" if type == :error

    return "+#{value}\r\n" if type == :simple

    return "$#{value.length}\r\n#{value}\r\n" if type == :bulk

    return unless type == :array && value.is_a?(Array)

    inner_content = value.map do |element|
      element_type = element.is_a?(Array) ? :array : :bulk
      encode_data(type: element_type, value: element)
    end.join

    "*#{value.length}\r\n#{inner_content}"
  end
end
