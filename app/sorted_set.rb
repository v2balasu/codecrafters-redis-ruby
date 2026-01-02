class SortedSet
  def initialize
    @lookup = {}
  end

  def insert(key:, value:)
    raise 'Value must be BigDecimal' unless value.is_a?(BigDecimal)

    @lookup[key] = value

    # TODO: Implement SkipList
    @lookup.sort
  end

  def count
    @lookup.count
  end
end
