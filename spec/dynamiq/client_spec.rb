require 'spec_helper'

describe Dynamiq::Client do
  let (:url) {'http://example.io'}
  let (:port) {'9999'}
  let (:conn) {Faraday.new(:url=>"#{url}:#{port}") }
  let (:response) {Faraday::Response.new({:status=>200, :body => '{}'})}
  subject (:client) {Dynamiq::Client.new(url,port)}

  context 'publish' do
    before (:each) do
      subject.stub(:connection).and_return(conn)
      conn.stub(:put)
    end

    it 'should PUT message to topic' do
      conn.should_receive(:put).with("/topics/topic/message", {:x=>'y'})
      subject.publish('topic', {:x=>'y'})
    end

    it 'should log with failure' do
      conn.stub(:put).and_raise
      Dynamiq.logger.should_receive(:error)
      expect(subject.publish('topic', {:x=>'y'})).to eq(false)
    end
  end

  context 'acknowledge' do
    before (:each) do
      Dynamiq::Client.any_instance.stub(:connection).and_return(conn)
      conn.stub(:delete)
    end

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
      Dynamiq::Client.any_instance.stub(:connection).and_return(conn)
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
      Dynamiq::Client.any_instance.stub(:connection).and_return(conn)
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
