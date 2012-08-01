module EventMachine
  module Protocols
    module Zmq2
      EMPTY = ''.freeze
      SMALL_TIMEOUT = 0.03
      HWM_INFINITY = 2**20
      autoload :PreDealer, 'em/protocols/zmq2/dealer.rb'
      autoload :Dealer, 'em/protocols/zmq2/dealer.rb'
      autoload :DealerCb, 'em/protocols/zmq2/dealer.rb'
      autoload :PreReq, 'em/protocols/zmq2/req.rb'
      autoload :Req, 'em/protocols/zmq2/req.rb'
      autoload :ReqCb, 'em/protocols/zmq2/req.rb'
      autoload :ReqDefer, 'em/protocols/zmq2/req.rb'
      autoload :PreRouter, 'em/protocols/zmq2/router.rb'
      autoload :Router, 'em/protocols/zmq2/router.rb'
      autoload :PreRep, 'em/protocols/zmq2/rep.rb'
      autoload :Rep, 'em/protocols/zmq2/rep.rb'
      autoload :Sub, 'em/protocols/zmq2/pub_sub.rb'
      autoload :PrePub, 'em/protocols/zmq2/pub_sub.rb'
      autoload :Pub, 'em/protocols/zmq2/pub_sub.rb'
    end
  end
end
require 'em/protocols/zmq2/socket_connection'
require 'em/protocols/zmq2/socket'

class String
  unless method_defined?(:byteslice)
    alias byteslice slice
  end
end
