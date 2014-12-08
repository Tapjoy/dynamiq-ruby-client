require 'spec_helper'

describe Dynamiq do
  subject {::Dynamiq}

  it "should respond to logger" do
    subject.respond_to?(:logger).should == true
  end
end
