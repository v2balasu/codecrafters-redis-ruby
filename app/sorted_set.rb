class SortedSet
  def initialize
    @lookup = {}
    @sort_list = []
  end

  def insert(members)
    new_entries = 0

    members.each do |member|
      key, value = member.values_at(:key, :value)
      raise 'Value must be BigDecimal' unless value.is_a?(BigDecimal)

      new_entries += 1 unless @lookup[key]
      @lookup[key] = value
    end

    # TODO: Implement SkipList
    @sort_list = @lookup.sort_by { |k, v| [v, k] }

    new_entries
  end

  def get_sort_index(key)
    @sort_list.index { |kv| kv.first == key }
  end

  def count
    @lookup.count
  end
end
