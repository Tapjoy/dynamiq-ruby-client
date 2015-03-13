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

Create a client

```ruby
@dynamiq = Dynamiq::Client.new('http://example.io', '9999')
```

Publish data to a topic

```ruby
@dynamiq.publish('my_topic', {:k=>'v'})
```

Ack a Dynamiq message
```ruby
@dynamiq.acknowledge('my_queue', 'a3df32')
```

Receive a batch of Dynamiq messages
```ruby
# with batch size of 20
@dynamiq.receive('my_queue', 20)
```

Read a Dynamiq queue details
```ruby
@dynamiq.queue_details('my_queue')
```

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
