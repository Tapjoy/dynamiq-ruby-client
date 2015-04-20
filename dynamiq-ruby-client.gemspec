# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'dynamiq/version'

Gem::Specification.new do |spec|
  spec.name          = "dynamiq-ruby-client"
  spec.version       = Dynamiq::VERSION
  spec.authors       = ["Tapjoy"]
  spec.email         = ["eng-group-arch@tapjoy.com"]
  spec.description   = %q{A Ruby client for Dynamiq}
  spec.summary       = %q{A Ruby client for Dynamiq}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "faraday"
  spec.add_runtime_dependency "rspec"
  spec.add_runtime_dependency "net-http-persistent"
  
  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
end
