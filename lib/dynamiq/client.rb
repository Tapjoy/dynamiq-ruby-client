require 'faraday'
require 'json'

module Dynamiq
  class Client
    def initialize(url, port)
      @url = url
      @port = port
    end

    # Publish to a Dynamiq topic
    # @param topic [String] name of the topic
    # @param data [Hash] message data
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.publish('my_topic', {:k=>'v'})
    # => 
    # true
    #
    def publish(topic, data)
      begin
        connection.put("/topics/#{topic}/message", data)
        true
      rescue => e
        Dynamiq.logger.error "an error occured when publishing #{e.inspect}"
        false
      end
    end

    # Ack a Dynamiq message
    # @param queue [String] name of the queue
    # @param message_id [String] id of the message
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.acknowledge('my_queue', 'a3df32')
    # => 
    # true
    #
    def acknowledge(queue, message_id)
      begin
        connection.delete("/queues/#{queue}/message/#{message_id}")
        true
      rescue => e
        Dynamiq.logger.error "an error occured when acknowledging #{e.inspect}"
        false
      end
    end

    # Receive a batch of Dynamiq messages
    # @param queue [String] name of the queue
    # @param batch_size [Integer] the size of the batch
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.receive('my_queue', 20)
    # => 
    # {...message data}
    #
    def receive(queue, batch_size=10)
      begin
        resp = connection.get("/queues/#{queue}/messages/#{batch_size}")
        return JSON.parse(resp.body) if resp.status == 200
        []
      rescue => e
        Dynamiq.logger.error "an error occured when acknowledging #{e.inspect}"
      end
    end

    # Read a Dynamiq queue details
    # @param queue [String] name of the queue
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.queue_details('my_queue')
    # => 
    # {...queue details}
    #
    def queue_details(queue)
      begin
        resp = connection.get("/queues/#{queue}")
        return JSON.parse(resp.body) if resp.status == 200
        nil
      rescue => e
        Dynamiq.logger.error "an error occured when acquiring details for queue #{queue} #{e.inspect}"
      end
    end

    def connection
      @connection || Faraday.new(:url=>"#{@url}:#{@port}")
    end
  end
end
