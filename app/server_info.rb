require 'securerandom'

class ServerInfo
  def initialize(master_host, master_port)
    @role = master_host.nil? ? 'master' : 'slave'
    return unless @role == 'master'

    @master_host = master_host
    @master_port = master_port
    @master_replid = SecureRandom.alphanumeric(40)
    @master_repl_offset = 0
  end

  def serialize
    {
      role: @role,
      master_replid: @master_replid,
      master_repl_offset: @master_repl_offset
    }.compact
      .map { |k, v| "#{k}:#{v}\n" }.join
  end
end
