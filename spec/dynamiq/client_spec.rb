require 'spec_helper'

describe Dynamiq::Client do
  let (:topic_name) {"test_topic"}
  let (:queue_name) {"test_queue"}
  let (:url) {'http://example.io'}
  let (:port) {9999}
  let (:response) {Faraday::Response.new({:status=>200, :body => '{}'})}
  let (:connection_timeout) {5}
  let (:client_options) {{:connection_timeout=>connection_timeout}}
  subject (:client) {Dynamiq::Client.new(url,port, client_options)}
  let(:conn) { subject.connection } 

  before :each do
    ::Dynamiq.logger.stub(:error)
  end

  context '#connection' do
    it 'includes url in url prefix' do
      expect(conn.url_prefix.to_s).to match(/^#{url}/)
    end

    it 'includes port in url prefix' do
      expect(conn.url_prefix.port).to eq(port)
    end

    it 'includes api version in url prefix' do
      expect(conn.url_prefix.to_s).to match(%r(/#{subject.class::API_VERSION}$))
    end
  end

  context '#initialize' do
    context "when a connection timeout is provided" do
      it "is set on the underlying Faraday object" do
        expect(conn.options.timeout).to eq(connection_timeout) 
      end
    end

    context "when no connection timeout is provided" do
      let(:client_options) {{}}
      it "uses the internally set default value" do
        expect(conn.options.timeout).to eq(subject.class::DEFAULT_CONNECTION_TIMEOUT) 
      end
    end
  end

  context '#create_topic' do
    it 'should PUT the topic name' do
      conn.should_receive(:put).with("topics/#{topic_name}")
      subject.create_topic(topic_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.create_topic(topic_name)
      end

      it 'should return false' do
        expect(subject.create_topic(topic_name)).to eq(false)
      end
    end
  end

  context '#create_queue' do
    it 'should PUT the queue name' do
      conn.should_receive(:put).with("queues/#{queue_name}")
      subject.create_queue(queue_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.create_queue(queue_name)
      end

      it 'should return false' do
        expect(subject.create_queue(queue_name)).to eq(false)
      end
    end
  end

  context '#delete_topic' do
    it 'should DELETE the topic name' do
      conn.should_receive(:delete).with("topics/#{topic_name}")
      subject.delete_topic(topic_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:delete).and_raise
      end

      it 'should log the error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.create_queue(queue_name)
      end

      it 'should return false' do
        expect(subject.create_queue(queue_name)).to eq(false)
      end
    end
  end

  context '#delete_queue' do
    it 'should DELETE the queue name' do
      conn.should_receive(:delete).with("queues/#{queue_name}")
      subject.delete_queue(queue_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:delete).and_raise
      end

      it 'should log the error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.create_queue(queue_name)
      end

      it 'should return false' do
        expect(subject.create_queue(queue_name)).to eq(false)
      end
    end
  end

  context '#subscribe_queue' do
    it 'should PUT the topic and queue names' do
      conn.should_receive(:put).with("topics/#{topic_name}/queues/#{queue_name}")
      subject.subscribe_queue(topic_name, queue_name)
    end

    context 'when code is not 200' do
      let(:response) { Faraday::Response.new({:status=>404, :body => '{}'}) }
      before(:each) do
        conn.stub(:put).and_return(response)
      end

      it 'should log error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.subscribe_queue(topic_name, queue_name)
      end

      it 'should return false' do
        expect(subject.subscribe_queue(topic_name, queue_name)).to eq(false)
      end
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.subscribe_queue(topic_name, queue_name)
      end

      it 'should return false' do
        expect(subject.subscribe_queue(topic_name, queue_name)).to eq(false)
      end
    end
  end

  context '#configure_queue' do
    let (:config_data) {
      {
        :visibility_timeout=>10,
        :min_partitions=>1,
        :max_partitions=>10
      }
    }

    it 'should PATCH the queue name with option data' do
      conn.should_receive(:patch)
      subject.configure_queue(queue_name, config_data)
    end

    context 'on failure' do
      before :each do
        conn.stub(:patch).and_raise
      end

      it 'should log the error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.configure_queue(queue_name, config_data)
      end

      it 'should return false' do
        expect(subject.configure_queue(queue_name, config_data)).to eq(false)
      end
    end
  end

  context '#publish' do
    context 'on success' do
      let (:response) {Faraday::Response.new({:status=>200, :body => '{"q1":"123", "q2":"456"}'})}
      before(:each) do
        conn.stub(:put).and_return(response)
      end

      it 'should PUT message to topic' do
        conn.should_receive(:put).with("topics/#{topic_name}/message", {:x=>'y'})
        subject.publish(topic_name, {:x=>'y'})
      end

      it 'should return an array of valid message ids' do
        resp = subject.publish(topic_name, {:x=>'y'})
        resp.should eq({"q1"=>"123", "q2"=>"456"})
      end
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.publish(topic_name, {:x=>'y'})
      end

      it 'should return an empty hash' do
        expect(subject.publish(topic_name, {:x=>'y'})).to eq({})
      end
    end
  end

  context '#enqueue' do
    context 'on success' do
      let (:response) {Faraday::Response.new({:status=>200, :body => "123"})}
      before(:each) do
        conn.stub(:put).and_return(response)
      end
      
      it 'should PUT message to the queue' do
        conn.should_receive(:put).with("queues/#{queue_name}/message", {:x=>'y'})
        subject.enqueue(queue_name, {:x=>'y'})
      end
  
      it 'should return a valid message id' do
        resp = subject.enqueue(queue_name, {:x=>'y'})
        resp.should eq("123")
      end
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        ::Dynamiq.logger.should_receive(:error)
        subject.enqueue(queue_name, {:x=>'y'})
      end

      it 'should return an empty string' do
        expect(subject.enqueue(queue_name, {:x=>'y'})).to eq("")
      end
    end
  end

  context '#acknowledge' do
    it 'should DELETE message from queue' do
      conn.should_receive(:delete).with("queues/q/message/id")
      subject.acknowledge('q', 'id')
    end

    it 'should log with failure' do
      conn.stub(:delete).and_raise
      ::Dynamiq.logger.should_receive(:error)
      expect(subject.acknowledge('q', 'id')).to eq(false)
    end
  end

  context '#receive' do
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    shared_examples 'should log with failure and raise exception' do
      it 'should log with failure and raise exception' do
        ::Dynamiq.logger.should_receive(:error)
        expect { subject.receive('q', 11) }.to raise_exception
      end
    end

    it 'should GET a message batch from queue' do
      conn.should_receive(:get).with("queues/q/messages/11")
      expect(subject.receive('q', 11)).to eq({})
    end

    context 'when status code is not 200' do
      let(:response_status) { 400 }
      let(:response) { Faraday::Response.new({:status=>response_status, :body => '{}'}) }

      include_examples 'should log with failure and raise exception'

      [404,422].each do |status|
        context "when status code is #{status}" do
          let(:response_status) { status }

          include_examples 'should log with failure and raise exception'

          it 'should raise ArgumentError' do
            expect { subject.receive('q', 11) }.to raise_exception(ArgumentError)
          end
        end
      end
    end

    context 'when an exception is raised in get' do
      before(:each) { conn.stub(:get).and_raise }

      include_examples 'should log with failure and raise exception'
    end
  end

  context '#queue_details' do
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET queue details' do
      conn.should_receive(:get).with("queues/q")
      expect(subject.queue_details('q')).to eq({})
    end

    it 'should return nil for non 200s' do
      r = Faraday::Response.new({:status=>400, :body => '{}'})
      conn.stub(:get).and_return(r)

      expect(subject.queue_details('q')).to eq(nil)
    end

    it 'should log warning for 404s' do
      r = Faraday::Response.new({:status=>404, :body => '{}'})
      conn.stub(:get).and_return(r)
      ::Dynamiq.logger.should_receive(:warn)
      subject.queue_details('q')
    end

    it 'should log with failure' do
      conn.stub(:get).and_raise
      ::Dynamiq.logger.should_receive(:error)
      subject.queue_details('q')
    end
  end

  context '#known_queues' do
    let (:response) {Faraday::Response.new({:status=>200, :body => '[]'})}
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET the known list of queues' do
      conn.should_receive(:get).with("queues")
      expect(subject.known_queues).to eq([])
    end

    it 'should return an empty array for non 200s' do
      r = Faraday::Response.new({:status=>404, :body => '{}'})
      conn.stub(:get).and_return(r)

      expect(subject.known_queues).to eq([])
    end

    it 'should log with failure' do
      conn.stub(:get).and_raise
      ::Dynamiq.logger.should_receive(:error)
      subject.known_queues
    end
  end

  context '#known_topics' do
    let (:response) {Faraday::Response.new({:status=>200, :body => '[]'})}
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET the known list of topics' do
      conn.should_receive(:get).with("topics")
      expect(subject.known_topics).to eq([])
    end

    it 'should return an empty array for non 200s' do
      r = Faraday::Response.new({:status=>404, :body => '{}'})
      conn.stub(:get).and_return(r)

      expect(subject.known_topics).to eq([])
    end

    it 'should log with failure' do
      conn.stub(:get).and_raise
      ::Dynamiq.logger.should_receive(:error)
      subject.known_topics
    end
  end
end
