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
    let (:response) {Faraday::Response.new({:status=>201, :body => ''})}
    before :each do
      conn.stub(:put).and_return(response)
    end

    it 'should PUT the topic name' do
      conn.should_receive(:put).with("topics/#{topic_name}")
      subject.create_topic(topic_name)
    end

    context 'on failure' do
      context "when the topic already existed" do
        let (:response) {Faraday::Response.new({:status=>422, :body => '{"error":"blah"}'})}

        it 'should raise an error' do
          expect{subject.create_topic(topic_name)}.to raise_error(Dynamiq::Client::ObjectAlreadyExistsError)
        end
      end
    end
  end

  context '#create_queue' do
    let (:response) {Faraday::Response.new({:status=>201, :body => ''})}
    before :each do
      conn.stub(:put).and_return(response)
    end

    it 'should PUT the queue name' do
      conn.should_receive(:put).with("queues/#{queue_name}")
      subject.create_queue(queue_name)
    end

    context 'on failure' do
      context "when the queue already existed" do
        let (:response) {Faraday::Response.new({:status=>422, :body => '{"error":"blah"}'})}

        it 'should raise an error' do
          expect{subject.create_queue(queue_name)}.to raise_error(Dynamiq::Client::ObjectAlreadyExistsError)
        end
      end
    end
  end

  context '#delete_topic' do
    let (:response) {Faraday::Response.new({:status=>200, :body => ''})}
    before :each do
      conn.stub(:delete).and_return(response)
    end

    it 'should DELETE the topic name' do
      conn.should_receive(:delete).with("topics/#{topic_name}")
      subject.delete_topic(topic_name)
    end

    context 'on failure' do
      context 'when the topic did not exist' do
        let (:response) {Faraday::Response.new({:status=>404, :body => '{"error":"error"}'})}

        it "should raise an ObjectDoesNotExistError" do
          expect {subject.delete_topic(topic_name)}.to raise_error(Dynamiq::Client::ObjectDoesNotExistError)
        end
      end

      context 'when there is something other than a 200 or 404' do
        let (:response) {Faraday::Response.new({:status=>500, :body => ''})}

        it "should raise a ConnectionError" do
          expect {subject.delete_topic(topic_name)}.to raise_error(Dynamiq::Client::ConnectionError)
        end
      end
    end
  end

  context '#subscribe_queue' do
    let(:response) { Faraday::Response.new({:status=>200, :body => '{"Queues":["q1","q2"]}'}) }
    before(:each) do
      conn.stub(:put).and_return(response)
    end

    it 'should PUT the topic and queue names' do
      conn.should_receive(:put).with("topics/#{topic_name}/queues/#{queue_name}")
      subject.subscribe_queue(topic_name, queue_name)
    end

    context 'on failure' do
      context 'when either the queue or topic do not exist' do
        let(:response) { Faraday::Response.new({:status=>422, :body => '{}'}) }
        it "raises an ObjectDoesNotExistError" do
          expect{subject.subscribe_queue(topic_name, queue_name)}.to raise_error(Dynamiq::Client::ObjectDoesNotExistError)
        end
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
    let(:response) { Faraday::Response.new({:status=>200, :body => '{}'}) }
    before(:each) do
      conn.stub(:patch).and_return(response)
    end

    it 'should PATCH the queue name with option data' do
      conn.should_receive(:patch)
      subject.configure_queue(queue_name, config_data)
    end

    context 'on non 200' do
      let(:response) { Faraday::Response.new({:status=>501, :body => '{}'}) }


      it 'should raise a ConnectionError' do
        expect{subject.configure_queue(queue_name, config_data)}.to raise_error(Dynamiq::Client::ConnectionError)
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
        expect(resp).to eq({"q1"=>"123", "q2"=>"456"})
      end
    end

    context 'on failure' do
      context 'in the transport layer' do
        before :each do
          conn.stub(:put).and_raise(Faraday::Error::ConnectionFailed.new(ArgumentError.new))
        end
  
        it 'should raise an error' do
          expect { subject.publish(queue_name, {:x=>'y'}) }.to raise_error(Dynamiq::Client::ConnectionError)
        end
      end

      context 'in the application layer' do      
        let (:bad_response) {Faraday::Response.new({:status=>500, :body => 'error'})}
        let (:good_response) {Faraday::Response.new({:status=>200, :body => '{"q1":"123", "q2":"456"}'})}

        let (:client_options) {{:connection_timeout=>connection_timeout, :retry_count=>2}}
        before(:each) do
          conn.stub(:put).and_return(bad_response, bad_response, good_response)
        end

        it 'will retry upto the configured number of times' do
          conn.should_receive(:put).exactly(3).times
          resp = subject.publish(topic_name, {:x=>'y'})
        end
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
        expect(resp).to eq("123")
      end
    end

    context 'on failure' do
      context 'in the transport layer' do
        before :each do
          conn.stub(:put).and_raise(Faraday::Error::ConnectionFailed.new(ArgumentError.new))
        end
  
        it 'should raise the error' do
          expect { subject.enqueue(queue_name, {:x=>'y'})}.to raise_error(Dynamiq::Client::ConnectionError)
        end
      end

      context 'in the application layer' do
        let (:bad_response) {Faraday::Response.new({:status=>500, :body => 'error'})}
        let (:good_response) {Faraday::Response.new({:status=>200, :body => '{"q1":"123", "q2":"456"}'})}

        let (:client_options) {{:connection_timeout=>connection_timeout, :retry_count=>2}}
        before(:each) do
          conn.stub(:put).and_return(bad_response, bad_response, good_response)
        end

        it 'will retry upto the configured number of times' do
          conn.should_receive(:put).exactly(3).times
          resp = subject.enqueue(topic_name, {:x=>'y'})
        end
      end
    end
  end

  context '#acknowledge' do
    before (:each) do
      conn.stub(:delete).and_return(response)
    end

    it 'should DELETE message from queue' do
      conn.should_receive(:delete).with("queues/q/message/id")
      subject.acknowledge('q', 'id')
    end
  end

  context "#acknowledge_many" do
    before(:each) do
      conn.stub(:delete).and_return(response)
    end

    it "should DELETE messages from queue" do
      conn.should_receive(:delete).with("queues/q/messages/id1,id2,id3")
      subject.acknowledge_many('q', ['id1','id2','id3'])
    end
  end

  context '#receive' do
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET a message batch from queue' do
      conn.should_receive(:get).with("queues/q/messages/11")
      expect(subject.receive('q', 11)).to eq({})
    end

    context 'when status code is not 200' do
      let(:response) { Faraday::Response.new({:status=>response_status, :body => '{}'}) }
      context "when status code is 404" do
        let(:response_status) { 404 }

        it 'should raise ObjectDoesNotExistError' do
          expect { subject.receive('q', 11) }.to raise_exception(Dynamiq::Client::ObjectDoesNotExistError)
        end
      end

      context "when status code is 422" do
        let(:response_status) { 422 }
        
        it 'should raise ArgumentError' do
          expect { subject.receive('q', 11) }.to raise_exception(ArgumentError)
        end
      end
    end
  end

  context '#queue_details' do
    let (:response) {Faraday::Response.new({:status=>200, :body => '{"option":"100"}'})}
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET queue details' do
      conn.should_receive(:get).with("queues/q")
      expect(subject.queue_details('q')).to eq({"option"=>"100"})
    end

    context "when the queue does not exist" do
      let (:response) {Faraday::Response.new({:status=>404, :body => ''})}
      it "raises an ObjectDoesNotExistError" do
        expect{subject.queue_details('q')}.to raise_exception(Dynamiq::Client::ObjectDoesNotExistError)
      end
    end

    context "when we receive a non 200" do
      let (:response) {Faraday::Response.new({:status=>500, :body => '{"option":"100"}'})}
      it "raises a ConnectionError" do
        expect{subject.queue_details('q')}.to raise_exception(Dynamiq::Client::ConnectionError)
      end
    end
  end

  context '#known_queues' do
    let (:response) {Faraday::Response.new({:status=>200, :body => '{"queues": ["q1","q2"]}'})}
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET the known list of queues' do
      conn.should_receive(:get).with("queues")
      expect(subject.known_queues).to eq(["q1","q2"])
    end

    it 'should raise an exception if we dont get a 200' do
      r = Faraday::Response.new({:status=>404, :body => '{}'})
      conn.stub(:get).and_return(r)

      expect{subject.known_queues}.to raise_exception(Dynamiq::Client::ConnectionError) 
    end
  end

  context '#known_topics' do
    let (:response) {Faraday::Response.new({:status=>200, :body => '{"topics": ["t1","t2"]}'})}
    before (:each) do
      conn.stub(:get).and_return(response)
    end

    it 'should GET the known list of topics' do
      conn.should_receive(:get).with("topics")
      expect(subject.known_topics).to eq(["t1","t2"])
    end

    it 'should raise an exception if we dont get a 200' do
      r = Faraday::Response.new({:status=>404, :body => '{}'})
      conn.stub(:get).and_return(r)

      expect{subject.known_topics}.to raise_exception(Dynamiq::Client::ConnectionError) 
    end
  end
end
