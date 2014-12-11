require 'spec_helper'

describe Dynamiq::Client do
  let (:topic_name) {"test_topic"}
  let (:queue_name) {"test_queue"}
  let (:url) {'http://example.io'}
  let (:port) {'9999'}
  let (:conn) {Faraday.new(:url=>"#{url}:#{port}") }
  let (:response) {Faraday::Response.new({:status=>200, :body => '{}'})}
  subject (:client) {Dynamiq::Client.new(url,port)}

  before :each do
    subject.stub(:connection).and_return(conn)
    Dynamiq.logger.stub(:error)
  end

  context 'create_topic' do
    it 'should PUT the topic name' do
      conn.should_receive(:put).with("/topics/#{topic_name}")
      subject.create_topic(topic_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        Dynamiq.logger.should_receive(:error)
        subject.create_topic(topic_name)
      end

      it 'should return false' do
        expect(subject.create_topic(topic_name)).to eq(false)
      end
    end
  end

  context 'create_queue' do
    it 'should PUT the queue name' do
      conn.should_receive(:put).with("/queues/#{queue_name}")
      subject.create_queue(queue_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        Dynamiq.logger.should_receive(:error)
        subject.create_queue(queue_name)
      end

      it 'should return false' do
        expect(subject.create_queue(queue_name)).to eq(false)
      end
    end
  end

  context 'delete_topic' do
    it 'should DELETE the topic name' do
      conn.should_receive(:delete).with("/topics/#{topic_name}")
      subject.delete_topic(topic_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:delete).and_raise
      end

      it 'should log the error' do
        Dynamiq.logger.should_receive(:error)
        subject.create_queue(queue_name)
      end

      it 'should return false' do
        expect(subject.create_queue(queue_name)).to eq(false)
      end
    end
  end

  context 'delete_queue' do
    it 'should DELETE the queue name' do
      conn.should_receive(:delete).with("/queues/#{queue_name}")
      subject.delete_queue(queue_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:delete).and_raise
      end

      it 'should log the error' do
        Dynamiq.logger.should_receive(:error)
        subject.create_queue(queue_name)
      end

      it 'should return false' do
        expect(subject.create_queue(queue_name)).to eq(false)
      end
    end
  end

  context 'subscribe_queue' do
    it 'should PUT the topic and queue names' do
      conn.should_receive(:put).with("/topics/#{topic_name}/queues/#{queue_name}")
      subject.subscribe_queue(topic_name, queue_name)
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        Dynamiq.logger.should_receive(:error)
        subject.subscribe_queue(topic_name, queue_name)
      end

      it 'should return false' do
        expect(subject.subscribe_queue(topic_name, queue_name)).to eq(false)
      end
    end
  end

  context 'configure_queue' do
    let (:config_data) {
      {
        :visibility_timeout=>10,
        :min_partitions=>1,
        :max_partitions=>10
      }
    }
    it 'should PATCH the queue name with option data' do
      conn.should_receive(:patch).with("/queues/#{queue_name}", config_data)
      subject.configure_queue(queue_name, config_data)
    end

    context 'on failure' do
      before :each do
        conn.stub(:patch).and_raise
      end

      it 'should log the error' do
        Dynamiq.logger.should_receive(:error)
        subject.configure_queue(queue_name, config_data)
      end

      it 'should return false' do
        expect(subject.configure_queue(queue_name, config_data)).to eq(false)
      end
    end
  end

  context 'publish' do
    it 'should PUT message to topic' do
      conn.should_receive(:put).with("/topics/#{topic_name}/message", {:x=>'y'})
      subject.publish(topic_name, {:x=>'y'})
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        Dynamiq.logger.should_receive(:error)
        subject.publish(topic_name, {:x=>'y'})
      end

      it 'should return false' do
        expect(subject.publish(topic_name, {:x=>'y'})).to eq(false)
      end
    end
  end

  context 'enqueue' do
    it 'should PUT message to the queue' do
      conn.should_receive(:put).with("/queues/#{queue_name}/message", {:x=>'y'})
      subject.enqueue(queue_name, {:x=>'y'})
    end

    context 'on failure' do
      before :each do
        conn.stub(:put).and_raise
      end

      it 'should log the error' do
        Dynamiq.logger.should_receive(:error)
        subject.enqueue(queue_name, {:x=>'y'})
      end

      it 'should return false' do
        expect(subject.enqueue(queue_name, {:x=>'y'})).to eq(false)
      end
    end
  end

  context 'acknowledge' do
    it 'should DELETE message from queue' do
      conn.should_receive(:delete).with("/queues/q/message/id")
      subject.acknowledge('q', 'id')
    end

    it 'should log with failure' do
      conn.stub(:delete).and_raise
      Dynamiq.logger.should_receive(:error)
      expect(subject.acknowledge('q', 'id')).to eq(false)
    end
  end

  context 'receive' do
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET a message batch from queue' do
      conn.should_receive(:get).with("/queues/q/messages/11")
      expect(subject.receive('q', 11)).to eq({})
    end

    it 'should return an empty array for non 200s' do
      r = Faraday::Response.new({:status=>404, :body => '{}'})
      conn.stub(:get).and_return(r)

      expect(subject.receive('q')).to eq([])
    end

    it 'should log with failure' do
      conn.stub(:get).and_raise
      Dynamiq.logger.should_receive(:error)
      subject.receive('q', 11)
    end
  end

  context 'queue_details' do
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET queue details' do
      conn.should_receive(:get).with("/queues/q")
      expect(subject.queue_details('q')).to eq({})
    end

    it 'should return nil for non 200s' do
      r = Faraday::Response.new({:status=>404, :body => '{}'})
      conn.stub(:get).and_return(r)

      expect(subject.queue_details('q')).to eq(nil)
    end

    it 'should log with failure' do
      conn.stub(:get).and_raise
      Dynamiq.logger.should_receive(:error)
      subject.queue_details('q')
    end
  end
end
