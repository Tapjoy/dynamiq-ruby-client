require 'faraday'
require 'json'

module Dynamiq
  class Client
    def initialize(url, port)
      @url = url
      @port = port
    end

    # Create a Dynamiq topic
    # @param topic [String] name of the topic
    # @param opts [Hash] optional parameters
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.create_topic('my_topic')
    # => 
    # true
    #
    def create_topic(topic, opts={})
      begin
        connection.put("/topics/#{topic}")
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when creating a topic #{e.inspect}"
        false
      end
    end

    # Create a Dynamiq queue
    # @param queue [String] name of the queue
    # @param opts [Hash] optional parameters
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.create_queue('my_queue')
    # => 
    # true
    #
    def create_queue(queue, opts={})
      begin
        connection.put("/queues/#{queue}")
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when creating a queue #{e.inspect}"
        false
      end
    end

    # Delete a Dynamiq topic
    # @param topic [String] name of the topic
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.delete_topic('my_topic')
    # => 
    # true
    #
    def delete_topic(topic)
      begin
        connection.delete("/topics/#{topic}")
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when deleting a topic #{e.inspect}"
        false
      end
    end

    # Delete a Dynamiq queue
    # @param queue [String] name of the queue
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.delete_queue('my_queue')
    # => 
    # true
    #
    def delete_queue(queue)
      begin
        connection.delete("/queues/#{queue}")
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when deleting a queue #{e.inspect}"
        false
      end
    end

    # Subscribe a queue to a topic
    # @param topic [String] name of the topic
    # @param queue [String] name of the queue
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.assign_queue('my_topic','my_queue')
    # => 
    # true
    #
    def subscribe_queue(topic, queue)
      begin
        connection.put("/topics/#{topic}/queues/#{queue}")
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when assigning a queue to a topic #{e.inspect}"
        false
      end
    end

    # Configure a queue
    # @param queue [String] name of the queue
    # @param opts [Integer] :visibility_timeout The number of seconds to wait before making a message available again
    # @param opts [Integer] :min_partitions The minimum number of partitions for a Queue to serve messages from
    # @param opts [Integer] :max_partitions The maximum number of partitions for a Queue to serve messages from
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.assign_queue('my_topic','my_queue')
    # => 
    # true
    #
    def configure_queue(queue, opts={})
      begin
        connection.patch("/queues/#{queue}", opts)
      rescue => e
        ::Dynamiq.logger.error "an error occured when updating the configuration for a queue #{e.inspect}"
        false
      end
    end

    # Publish to a Dynamiq topic, which will enqueue to all subscribed queues
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
        ::Dynamiq.logger.error "an error occured when publishing #{e.inspect}"
        false
      end
    end

    # Enqueue to a Dynamiq queue directly
    # @param queue [String] name of the queue
    # @param data [Hash] message data
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.publish('my_topic', {:k=>'v'})
    # => 
    # true
    #
    def enqueue(queue, data)
      begin
        connection.put("/queues/#{queue}/message", data)
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when publishing #{e.inspect}"
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
        ::Dynamiq.logger.error "an error occured when acknowledging #{e.inspect}"
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
        ::Dynamiq.logger.error "an error occured when acknowledging #{e.inspect}"
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
        ::Dynamiq.logger.error "an error occured when acquiring details for queue #{queue} #{e.inspect}"
      end
    end

    def connection
      @connection || Faraday.new(:url=>"#{@url}:#{@port}")
    end
  end
end
