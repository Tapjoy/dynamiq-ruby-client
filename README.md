# Dynamiq Ruby Client

DynamiqClient provides a Ruby interface for the Dynamiq RESTful API 

## Installation

Add this line to your application's Gemfile:

    gem 'dynamiq-ruby-client'

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install dynamiq-ruby-client

## Usage

Creating a client is simple. You just need the address and port of one of your Dynamiq cluster boxes.

```ruby
dynamiq = Dynamiq::Client.new('http://example.io', '8081')
```

There are a few basic workflows to think about when working with Dynamiq. For an overview of the project itself, please see [Dynamiq](http://github.com/Tapjoy/dynamiq)

### Configuration

Create a topic

```ruby
dynamiq.create_topic('my_topic')
```

Create a queue

```ruby
dynamiq.create_queue('my_queue')
```

Subcribe the topic to the queue

```ruby
dynamiq.subscribe_queue('my_topic','my_queue')
```

Read the current configuration from a queue

```ruby
dynamiq.queue_details('my_queue')
```

Configure the queue

```ruby
dynamiq.configure_queue('my_queue', {
  :visibility_timeout=>5,
  :compressed_messages=>false,
  :min_partiitions=>1,
  :max_partitions=>10,
  :max_partition_age=>432000
})
```

The list of available configuration options may change, so refer to the Dynamiq readme page to be kept upto date. The Ruby client simply passes through any option you set, and does not attempt to validate the option names at this time. This prevents the need for a client update as new options are released, but provides no runtime protection against incorrect option names. Dynamiq will accept options it recongizes, and silently ignore those it does not.

### Publishing Data

The dynamiq client expects the data being published to be in the form of a string. Commonly, this would be JSON, but you're able to use any format that can be serialized effectively over HTTP.

You can publish data to a queue directly

```ruby
dynamiq.enqueue('my_queue', "{'k':'v'}")
```

Or, indirectly publish data to a topic which acts as a fanout to all subscribed queues

```ruby
dynamiq.publish('my_topic', "{'k':'v'}")
```

### Consuming and Acknowledging

Receive a batch of Dynamiq messages (for example, 20)

```ruby
dynamiq.receive('my_queue', 20)
```

This will return an array of messages, like so

```ruby
[{"body":"{'k':'v'}","id":"7096414283680965447"},{"body":"{'k2':'v2'}","id":"7447159266230116988"}]
```

Note that the batch size is a suggested max return value, and is not a gaurantee that you'll get your batch size fulfilled. 

You will never receive more than the batch size.

Acknolwedge / Delete a Dynamiq message

Once you have a message from #receive, you can take the "id" from the message, and use it in a call to #acknowledge to successfully delete the message from Dynamiq

```ruby
dynamiq.acknowledge('my_queue', '7447159266230116988')
```

