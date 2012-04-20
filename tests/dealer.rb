require 'eventmachine'
require 'ffi-rzmq'
require 'minitest/autorun'

require 'em/protocols/zmq2/dealer'

describe 'Dealer' do
  ZBIND_ADDR = 'tcp://127.0.0.1:7890'
  ZCONNECT_ADDR = 'tcp://127.0.0.1:7891'

  before do
    @zctx = ZMQ::Context.new
    @zbind = @zctx.socket(ZMQ::ROUTER)
    @zbind.identity = 'BIND_ROUTE'
    @zbind.bind(ZBIND_ADDR)
    @zconnect = @zctx.socket(ZMQ::ROUTER)
    @zconnect.identity = 'CONNECT_ROUTE'
    @zconnect.connect(ZCONNECT_ADDR)
  end

  after do
    @zbind.setsockopt(ZMQ::LINGER, 0)
    @zconnect.setsockopt(ZMQ::LINGER, 0)
    @zbind.close
    @zconnect.close
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
      def receive_message(message)
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
        dealer.connect(ZBIND_ADDR)
        dealer.bind(ZCONNECT_ADDR)
        defered.callback {
          messages.each{|message|
            dealer.send_message(message).must_equal true
          }
          EM.add_timer(0.1){ EM.stop }
        }
      }
      results = []
      result = []
      while @zbind.recv_strings(result, ZMQ::NOBLOCK) != -1
        result.shift.must_equal 'MyDealer'
        results << result
        result = []
      end
      results.size.must_be :>, 0
      results.size.must_be :<, messages.size
      while @zconnect.recv_strings(result, ZMQ::NOBLOCK) != -1
        result.shift.must_equal 'MyDealer'
        results << result
        result = []
      end
      (messages - results).must_be_empty
      (results - messages).must_be_empty
    end

    it "should be able to receive messages" do
      EM.run {
        dealer.connect(ZBIND_ADDR)
        dealer.bind(ZCONNECT_ADDR)
        defered.callback {
          EM.defer do
            messages[0...(messages.size/2)].each{|message|
              @zbind.send_strings ['MyDealer', *message]
            }
            messages[(messages.size/2)..-1].each{|message|
              @zconnect.send_strings ['MyDealer', *message]
            }
          end
        }
        EM.add_timer(1) { dealer.close;  EM.stop }
        # there is error somewhere
        EM.add_timer(1.1) { dealer.close;  EM.stop }
      }
      dealer.incoming_queue.size.must_equal messages.size
      (dealer.incoming_queue - messages).must_be_empty
      (messages - dealer.incoming_queue).must_be_empty
    end
  end
end
