require 'base64'

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

  def to_rdb_bytes
    # TODO: Hardcoding a empty DB for now, serialzie to full RDB later
    Base64.decode64(
      'UkVESVMwMDEx+glyZWRpcy13ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=='
    ).bytes
  end
end
