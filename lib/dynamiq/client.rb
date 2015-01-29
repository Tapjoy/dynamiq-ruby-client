require 'faraday'
require 'json'

module Dynamiq
  class Client
    DEFAULT_CONNECTION_TIMEOUT = 2

    class ConnectionError < Faraday::Error::ConnectionFailed; end
    class TimeoutError < Faraday::Error::TimeoutError; end

    attr_reader :connection_timeout
    def initialize(url, port, opts={})
      @url = url
      @port = port
      @connection_timeout = opts[:connection_timeout] || DEFAULT_CONNECTION_TIMEOUT
    end

    # Create a Dynamiq topic, if it does not already exist on the server
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
        handle_errors { connection.put("/topics/#{topic}") }
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when creating a topic - #{e.inspect}: #{e.message}"
        false
      end
    end

    # Create a Dynamiq queue, if it does not already exist on the server
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
        handle_errors { connection.put("/queues/#{queue}") }
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when creating a queue - #{e.inspect}: #{e.message}"
        false
      end
    end

    # Delete a Dynamiq topic, if it exists on the server
    # @param topic [String] name of the topic
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.delete_topic('my_topic')
    # => 
    # true
    #
    def delete_topic(topic)
      begin
        handle_errors { connection.delete("/topics/#{topic}") }
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when deleting a topic - #{e.inspect}: #{e.message}"
        false
      end
    end

    # Delete a Dynamiq queue, if it exists on the server
    # @param queue [String] name of the queue
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.delete_queue('my_queue')
    # => 
    # true
    #
    def delete_queue(queue)
      begin
        handle_errors { connection.delete("/queues/#{queue}") }
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when deleting a queue - #{e.inspect}: #{e.message}"
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
        resp = handle_errors { connection.put("/topics/#{topic}/queues/#{queue}") }
        return true if resp.status == 200
        ::Dynamiq.logger.error "an error occured when assigning a queue to a topic - status code #{resp.status}: #{resp.body}"
        false
      rescue => e
        ::Dynamiq.logger.error "an error occured when assigning a queue to a topic - #{e.inspect}: #{e.message}"
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
        handle_errors do
          connection.patch do |req|
            req.url "/queues/#{queue}"
            req.headers["Content-Type"] = "application/json"
            req.body = JSON.dump(opts)
          end
        end
      rescue => e
        ::Dynamiq.logger.error "an error occured when updating the configuration for a queue - #{e.inspect}: #{e.message}"
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
        handle_errors { connection.put("/topics/#{topic}/message", data) }
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when publishing - #{e.inspect}: #{e.message}"
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
        handle_errors { connection.put("/queues/#{queue}/message", data) }
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when publishing - #{e.inspect}: #{e.message}"
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
        handle_errors { connection.delete("/queues/#{queue}/message/#{message_id}") }
        true
      rescue => e
        ::Dynamiq.logger.error "an error occured when acknowledging - #{e.inspect}: #{e.message}"
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
        resp = handle_errors { connection.get("/queues/#{queue}/messages/#{batch_size}") }
        return JSON.parse(resp.body) if resp.status == 200
        raise ArgumentError, resp.body if [404,422].include?(resp.status)
        raise StandardError, resp.body
      rescue => e
        ::Dynamiq.logger.error "an error occured when receiving messages - #{e.inspect}: #{e.message}"
        raise
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
        resp = handle_errors { connection.get("/queues/#{queue}") }
        return JSON.parse(resp.body) if resp.status == 200
        if resp.status == 404
          ::Dynamiq.logger.warn "tried to acquire details for queue '#{queue}' which does not exist"
        end
        nil
      rescue => e
        ::Dynamiq.logger.error "an error occured when acquiring details for queue '#{queue}' - #{e.inspect}: #{e.message}"
        nil
      end
    end

    # List known Dynamiq queues
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.known_queues
    # => 
    # [...queues]
    #
    def known_queues
      begin
        resp = handle_errors { connection.get("/queues") }
        return JSON.parse(resp.body)["queues"] if resp.status == 200
        []
      rescue => e
        ::Dynamiq.logger.error "an error occured when listing the known queues - #{e.inspect}: #{e.message}"
        []
      end
    end

    # List known Dynamiq topics
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.known_topics
    # => 
    # [...topics]
    #
    def known_topics
      begin
        resp = handle_errors { connection.get("/topics") }
        return JSON.parse(resp.body)["topics"] if resp.status == 200
        []
      rescue => e
        ::Dynamiq.logger.error "an error occured when listing the known topics - #{e.inspect}: #{e.message}"
        []
      end
    end

    def connection
      @connection ||= Faraday.new(:url=>"#{@url}:#{@port}") do |c|
        c.options.timeout = self.connection_timeout
        c.adapter  Faraday.default_adapter
      end
    end

    def handle_errors
      yield
    rescue Faraday::Error::ConnectionFailed => e
      raise ConnectionError, e
    rescue Faraday::Error::TimeoutError => e
      raise TimeoutError, e
    end
  end
end
