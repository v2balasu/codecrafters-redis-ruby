require 'singleton'

class SubscriptionManager
  include Singleton

  def initialize
    @channel_subscriptions = {}
    @client_callbacks = {}
  end

  def register_client(client_id:, publish_callback:)
    @client_callbacks[client_id] = publish_callback
  end

  def subscribe(client_id:, channel_name:)
    @channel_subscriptions[channel_name] ||= Set.new
    @channel_subscriptions[channel_name].add(client_id)
  end

  def count_client_subscriptions(client_id:)
    @channel_subscriptions
      .values
      .select { |subs| subs.include?(client_id) }
      .count
  end

  def client_count(channel_name:)
    @channel_subscriptions[channel_name]&.count || 0
  end
end
