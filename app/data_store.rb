require 'base64'
require_relative 'rdb_reader'

class DataStore
  attr_reader :rdb_dir, :rdb_fname

  def initialize(rdb_dir, rdb_fname)
    @store = {}
    @mutex = Thread::Mutex.new
    @stream_mutex = Thread::Mutex.new
    @rdb_dir = rdb_dir
    @rdb_fname = rdb_fname

    return unless rdb_dir && rdb_fname

    begin
      db = RDBReader.execute(dir: rdb_dir, filename: rdb_fname)
      @store = db[:databases]&.first || {}
    rescue StandardError => e
      pp "Error parsing db file: #{e.message}"
      @store = {}
    end
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

  def with_stream_lock(stream_key)
    @stream_mutex.synchronize do
      stream = get(stream_key) || []
      current_entry_id = stream.count.zero? ? nil : stream.last[:id]

      yield(stream, current_entry_id)

      set(stream_key, stream, nil)

      stream.last[:id]
    end
  end

  def update(key, value)
    @mutex.synchronize do
      return unless @store[key]

      @store[key][:value] = value
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

  def keys
    @mutex.synchronize do
      @store.reject! do |_k, v|
        !v[:expires_at].nil? && v[:expires_at] < Time.now
      end

      @store.keys
    end
  end

  def to_rdb
    # TODO: Hardcoding a empty DB for now, serialzie to full RDB later
    Base64.decode64(
      'UkVESVMwMDEx+glyZWRpcy13ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=='
    )
  end
end
