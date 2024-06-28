class DataStore
  def initialize
    @store = {}
    @mutex = Thread::Mutex.new
  end

  def set(key, value, expiry_seconds)
    @mutex.synchronize do
      item = {
        value: value,
        expiry_seconds: expiry_seconds
      }

      item[:expires_at] = Time.now + expiry_seconds unless expiry_seconds.nil?

      @store[key] = item
    end
  end

  def get(key)
    @mutex.synchronize do
      item = @store[key]
      return nil unless item

      if !item[:expires_at].nil? && item[:expires_at] < Time.now
        @store.delete(key)
        return
      end

      return item[:value]
    end
  end
end
