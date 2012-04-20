# -*- encoding: utf-8 -*-
require File.expand_path('../lib/em-zmq-tp10', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Sokolov Yura 'funny-falcon'"]
  gem.email         = ["funny.falcon@gmail.com"]
  gem.description   = %q{Implementation of ZMQ2.x transport protocol - ZMTP1.0}
  gem.summary       = %q{
Implementation of ZMQ2.1 transport protocol and main socket types using EventMachine
for managing TCP/UNIX connections, doing nonblocking writes and calling callback on
incoming messages.}
  gem.homepage      = "https://github.com/funny-falcon/em-zmq-tp10"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "em-zmq-tp10"
  gem.require_paths = ["lib"]
  gem.version       = Em::Protocols::Zmq2::VERSION
  gem.add_runtime_dependency 'eventmachine'
  gem.add_development_dependency 'ffi-rzmq'
  gem.required_ruby_version = '>= 1.9.2'
end
