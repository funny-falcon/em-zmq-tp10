require 'eventmachine'
require 'ffi-rzmq'
require 'minitest/autorun'

require 'em/protocols/zmq2/dealer'

describe 'Dealer' do
  ZBIND_ADDR = 'tcp://127.0.0.1:7890'
  ZCONNECT_ADDR = 'tcp://127.0.0.1:7891'

  def setup_native
    @zctx = ZMQ::Context.new
    @zbind = @zctx.socket(ZMQ::ROUTER)
    @zbind.identity = 'BIND_ROUTE'
    @zbind.bind(ZBIND_ADDR)
    @zconnect = @zctx.socket(ZMQ::ROUTER)
    @zconnect.identity = 'CONNECT_ROUTE'
    @zconnect.connect(ZCONNECT_ADDR)
  end

  def close_native
    @zbind.setsockopt(ZMQ::LINGER, 0)
    @zconnect.setsockopt(ZMQ::LINGER, 0)
    @zbind.close
    @zconnect.close
    @zctx.terminate
  end

  def with_native
    setup_native
    yield
  ensure
    close_native
  end

  let(:defered) do
    EM::DefaultDeferrable.new
  end

  describe EM::Protocols::Zmq2::PreDealer do
    class MyPreDealer < EM::Protocols::Zmq2::PreDealer
      attr :incoming_queue
      def initialize(opts={})
        super(opts)
        @defered = opts[:defered]
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

    let(:dealer) do
      MyPreDealer.new(identity: 'MyDealer', defered: defered)
    end

    let(:messages) do
      100.times.map{|n| ['', 'hello', 'world'] << n.to_s}
    end

    it "should be able to send message" do
      results = []
      with_native do
        EM.run {
          p defered.instance_variable_get("@defered_status")
          dealer.connect(ZBIND_ADDR)
          dealer.bind(ZCONNECT_ADDR)
          defered.callback {
            messages.each{|message|
              dealer.send_message(message).must_equal true
            }
            EM.add_timer(0.1){
              EM.next_tick{ EM.stop }
            }
          }
        }
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
      end
      (messages - results).must_be_empty
      (results - messages).must_be_empty
    end

    it "should be able to receive messages" do
      with_native do
        EM.run {
          dealer.connect(ZBIND_ADDR)
          dealer.bind(ZCONNECT_ADDR)
          defered.callback {
            messages[0...(messages.size/2)].each{|message|
              @zbind.send_strings ['MyDealer', *message]
            }
            messages[(messages.size/2)..-1].each{|message|
              @zconnect.send_strings ['MyDealer', *message]
            }
          }
          cb = proc do
            if dealer.incoming_queue.size < messages.size
              EM.add_timer(0.5, cb)
            else
              EM.next_tick{ EM.stop }
            end
          end
          cb.call
        }
      end
      dealer.incoming_queue.size.must_equal messages.size
      (dealer.incoming_queue - messages).must_be_empty
      (messages - dealer.incoming_queue).must_be_empty
    end

    it "should be able to connect after timeout" do
      connected = false
      EM.run do
        dealer.connect(ZBIND_ADDR)
        dealer.bind(ZCONNECT_ADDR)
        defered.callback{ connected = true;
          EM.next_tick{ EM.stop }
        }
        EM.add_timer(0.1) do
          EM.defer(proc do
            with_native{ sleep(0.1); }
          end)
        end
      end
      connected.must_equal true
    end
  end

  describe EM::Protocols::Zmq2::PreDealer do
    class MyDealer < EM::Protocols::Zmq2::Dealer
      attr :incoming_queue
      def initialize(opts={})
        super(opts)
        @defered = opts[:defered]
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

    let(:dealer) do
      MyDealer.new(identity: 'MyDealer', defered: defered)
    end

    let(:messages) do
      10000.times.map{|n| ['', 'hello', 'world'] << n.to_s}
    end

    it "should be able to send a lot of messages" do
      results_a = []
      results_b = []
      sizes = lambda{ results_a.size + results_b.size }
      with_native do
        thrd = Thread.new do
          result = []
          while sizes.call < messages.size
            while @zbind.recv_strings(result, ZMQ::NOBLOCK) != -1
              result.shift.must_equal 'MyDealer'
              results_a << result
              result = []
            end
            while @zconnect.recv_strings(result, ZMQ::NOBLOCK) != -1
              result.shift.must_equal 'MyDealer'
              results_b << result
              result = []
            end
            sleep(0.01)
          end
        end
        EM.run {
          p defered.instance_variable_get("@defered_status")
          dealer.connect(ZBIND_ADDR)
          dealer.bind(ZCONNECT_ADDR)
          defered.callback {
            messages.each{|message|
              dealer.send_message(message).must_equal true
            }
            cb = lambda do
              if sizes.call < messages.size
                EM.add_timer(0.5, cb)
              else
                EM.next_tick{ EM.stop }
              end
            end
            cb.call
          }
        }
        thrd.join
      end
      sizes.call.must_equal messages.size
      results_a.size.must_be :>, 0
      results_b.size.must_be :<, messages.size
      (messages - results_a - results_b).must_be_empty
      (results_a + results_b - messages).must_be_empty
    end
  end
end
