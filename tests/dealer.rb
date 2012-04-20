require 'eventmachine'
require 'ffi-rzmq'
require 'minitest/autorun'

require 'em/protocols/zmq2/dealer'

describe 'Dealer' do
  ZREP_ADDR = 'tcp://127.0.0.1:7890'
  ZROUTER_ADDR = 'tcp://127.0.0.1:7891'

  before do
    @zctx = ZMQ::Context.new
    @zrep = @zctx.socket(ZMQ::ROUTER)
    @zrep.identity = 'BIND_ROUTE'
    @zrep.bind(ZREP_ADDR)
    @zrouter = @zctx.socket(ZMQ::ROUTER)
    @zrouter.identity = 'CONNECT_ROUTE'
    @zrouter.connect(ZROUTER_ADDR)
  end

  after do
    @zrep.setsockopt(ZMQ::LINGER, 0)
    @zrouter.setsockopt(ZMQ::LINGER, 0)
    @zrep.close
    @zrouter.close
    @zctx.terminate
  end

  describe EM::Protocols::Zmq2::PreDealer do
    class MyDealer < EM::Protocols::Zmq2::PreDealer
      attr :incoming_queue
      def initialize(opts={}, defered)
        super(opts)
        @defered = defered
        @incoming_queue = []
      end
      def peer_free(peer_ident, connection)
        super
        @defered.succeed  if @free_peers.size == 2
      end
      def recieve_message(message)
        @incoming_queue << message
      end
    end

    let(:defered) do
      EM::DefaultDeferrable.new
    end

    let(:dealer) do
      MyDealer.new({identity: 'MyDealer'}, defered)
    end

    let(:messages) do
      messages = 100.times.map{|n| ['', 'hello', 'world'] << n.to_s}
    end

    it "should be able to send message" do
      EM.run {
        dealer.connect(ZREP_ADDR)
        dealer.bind(ZROUTER_ADDR)
        defered.callback {
          messages.each{|message|
            dealer.send_message(message).must_equal true
          }
          EM.add_timer(0.1){ EM.stop }
        }
      }
      results = []
      result = []
      while @zrep.recv_strings(result, ZMQ::NOBLOCK) != -1
        result.shift.must_equal 'MyDealer'
        results << result
        result = []
      end
      results.size.must_be :>, 0
      results.size.must_be :<, messages.size
      while @zrouter.recv_strings(result, ZMQ::NOBLOCK) != -1
        result.shift.must_equal 'MyDealer'
        results << result
        result = []
      end
      (messages - results).must_be_empty
      (results - messages).must_be_empty
    end
  end
end
