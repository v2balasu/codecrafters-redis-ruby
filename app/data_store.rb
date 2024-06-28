class DataStore
  def initialize
    @store = {}
    @mutex = Thread::Mutex.new
  end

  def set(key, value)
    @mutex.synchronize { @store[key] = value }
  end

  def get(key)
    @mutex.synchronize { @store[key] }
  end
end
