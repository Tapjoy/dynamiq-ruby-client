require 'faraday'
require 'json'

module Dynamiq
  class Client
    # 100ms connection timeout
    DEFAULT_CONNECTION_TIMEOUT = 0.1
    DEFAULT_RETRY_COUNT = 2
    API_VERSION = 'v1'

    class DynamiqError < RuntimeError; end
    class ConnectionError < DynamiqError; end
    class TimeoutError < DynamiqError; end
    class MessageDeliveryError < DynamiqError; end
    class MessageAcknowledgementError < DynamiqError; end
    class ObjectDoesNotExistError < DynamiqError; end
    class ObjectAlreadyExistsError < DynamiqError; end

    attr_reader :connection_timeout, :retry_count, :persistent_connection

    def initialize(url, port, opts={})
      @url = url
      @port = port
      @connection_timeout = opts[:connection_timeout] || DEFAULT_CONNECTION_TIMEOUT
      @retry_count = opts[:retry_count] || DEFAULT_RETRY_COUNT
      # Can't use the || idiom if the value is false
      @persistent_connection = opts.fetch(:persistent_connection, true)
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
      resp = retry_unless([201,422]) { connection.put("topics/#{topic}") }
      raise ObjectAlreadyExistsError, JSON.parse(resp.body)["error"] if resp.status == 422
      raise ConnectionError, "Failed to create topic. status: #{resp.status} response: #{resp.body}" unless resp.status == 201
      # No meaningful data in response
      true
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
      resp = retry_unless([201,422]) { connection.put("queues/#{queue}") } 
      raise ObjectAlreadyExistsError, JSON.parse(resp.body)["error"] if resp.status == 422
      raise ConnectionError, "Failed to create queue. status: #{resp.status} response: #{resp.body}" unless resp.status == 201
      # No meaningful data in response
      true
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
      resp = retry_unless([200,404]) { connection.delete("topics/#{topic}") }
      raise ObjectDoesNotExistError, JSON.parse(resp.body) if resp.status == 404
      raise ConnectionError, "Failed to create queue. status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      true
    end

    # Subscribe a queue to a topic
    # @param topic [String] name of the topic
    # @param queue [String] name of the queue
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.assign_queue('my_topic','my_queue')
    # => 
    # List of subscribed queues
    #
    def subscribe_queue(topic, queue)
      resp = retry_unless([200,422]) { connection.put("topics/#{topic}/queues/#{queue}") } 
      raise ObjectDoesNotExistError, JSON.parse(resp.body)["error"] if resp.status == 422
      raise ConnectionError, "Failed to subscribe queue to topic. status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      JSON.parse(resp.body)["Queues"]
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
      resp = retry_unless(200) do
        connection.patch do |req|
          req.url "queues/#{queue}"
          req.headers["Content-Type"] = "application/json"
          req.body = JSON.dump(opts)
        end
      end
      raise ConnectionError, "Failed to configure queue. status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      # There is no valuable information in the request body
      true
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
      resp = retry_unless(200) { connection.put("topics/#{topic}/message", data) } 
      raise MessageDeliveryError, "status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      JSON.parse(resp.body)
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
      resp = retry_unless(200) { connection.put("queues/#{queue}/message", data) } 
      raise MessageDeliveryError, "status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      resp.body 
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
      resp = retry_unless(200) { connection.delete("queues/#{queue}/message/#{message_id}") } 
      raise MessageAcknowledgementError, "status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      # There is no valuable information in the request body
      true
    end

    # Ack multiple Dynamiq messages
    # @param queue [String] name of the queue
    # @param message_ids [Array] ids of the messages
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.acknowledge_many('my_queue', ['a3df32','zz43545fd','hd65343'])
    # => 
    # {"deleted":3}
    #
    def acknowledge_many(queue, message_ids)
      resp = retry_unless(200) { connection.delete("queues/#{queue}/messages/#{message_ids.join(',')}") } 
      raise MessageAcknowledgementError, "status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      # There is no valuable information in the request body
      JSON.parse(resp.body)
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
      resp = retry_unless([200,404,422]) { connection.get("queues/#{queue}/messages/#{batch_size}") } 
      raise ArgumentError, "status: #{resp.status} response: #{resp.body}" if resp.status == 422
      raise ObjectDoesNotExistError, "status: #{resp.status} response: #{resp.body}" if resp.status == 404
      raise StandardError, "status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      JSON.parse(resp.body)
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
      resp = retry_unless([200,404]) { connection.get("queues/#{queue}") } 
      raise ObjectDoesNotExistError, "status: #{resp.status} response: #{resp.body}" if resp.status == 404
      raise ConnectionError, "Failed to get queue details. status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      JSON.parse(resp.body)
    end

    # List known Dynamiq queues
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.known_queues
    # => 
    # [...queues]
    #
    def known_queues
      resp = retry_unless(200) { connection.get("queues") } 
      raise ConnectionError, "Failed to list known queues. status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      JSON.parse(resp.body)["queues"]
    end

    # List known Dynamiq topics
    # @example
    #   @rqs = Dynamiq::Client.new('http://example.io', '9999')
    #   @rqs.known_topics
    # => 
    # [...topics]
    #
    def known_topics
      resp = retry_unless(200) { connection.get("topics") } 
      raise ConnectionError, "Failed to list known topics. status: #{resp.status} response: #{resp.body}" unless resp.status == 200
      JSON.parse(resp.body)["topics"]
    end

    def connection
      @connection ||= Faraday.new(:url=>"#{@url}:#{@port}") do |c|
        c.options.timeout = self.connection_timeout
        # There is a bug somewhere that causes Faraday Retry Middleware to retry 2 * max+1
        # 0 will result in 2 calls (original + 1 retry), which is closer to what we intend
        c.request :retry, :max => 0
        if persistent_connection
          c.adapter :net_http_persistent
        else
          c.adapter Faraday.default_adapter
        end
        c.path_prefix = API_VERSION
      end
    end

    def retry_unless(status_code)
      # Don't modify a value the user passes in
      codes = status_code
      # If it was only a single code passed in, make it an array
      codes = [codes] unless codes.is_a?(Array)

      retries_left = self.retry_count
      result = yield
      # If we haven't seen a known status code, and we have retries left, keep trying
      while !codes.include?(result.status) && retries_left > 0
        retries_left = retries_left - 1
        # Because we're not using a rescue, we can't use the retry keyword
        result = yield
      end
      return result
      rescue Faraday::Error::ConnectionFailed => e
        raise ConnectionError, e
      rescue Faraday::Error::TimeoutError => e
        raise TimeoutError, e
    end
  end
end
