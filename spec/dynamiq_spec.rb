require 'spec_helper'

describe Dynamiq do
  subject {::Dynamiq}

  it "should respond to logger" do
    expect(subject.respond_to?(:logger)).to eq(true)
  end
end
