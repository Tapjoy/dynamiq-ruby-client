require "dynamiq/version"
require 'dynamiq/client'
require 'logger'

module Dynamiq
  def self.logger
    @logger ||= Logger.new(STDOUT)
  end
end
