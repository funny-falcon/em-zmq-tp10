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

  let(:connected) do
    EM::DefaultDeferrable.new
  end

  describe EM::Protocols::Zmq2::PreDealer do
    class MyPreDealer < EM::Protocols::Zmq2::PreDealer
      attr :incoming_queue
      def initialize(opts={})
        super(opts)
        @connected = opts[:connected]
        @incoming_queue = []
      end
      def peer_free(peer_ident, connection)
        super
        @connected.succeed  if @free_peers.size == 2
      end
      def receive_message(message)
        @incoming_queue << message
      end
    end

    let(:dealer) do
      MyPreDealer.new(identity: 'MyDealer', connected: connected)
    end

    let(:messages) do
      100.times.map{|n| ['', 'hello', 'world', n.to_s]}
    end

    it "should be able to send message" do
      results = []
      with_native do
        EM.run {
          dealer.connect(ZBIND_ADDR)
          dealer.bind(ZCONNECT_ADDR)
          connected.callback {
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
          connected.callback {
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
      connected_ = false
      EM.run do
        dealer.connect(ZBIND_ADDR)
        dealer.bind(ZCONNECT_ADDR)
        connected.callback{ connected_ = true;
          EM.next_tick{ EM.stop }
        }
        EM.add_timer(0.1) do
          EM.defer(proc do
            with_native{ sleep(0.1); }
          end)
        end
      end
      connected_.must_equal true
    end
  end

  describe EM::Protocols::Zmq2::PreDealer do
    class MyDealer < EM::Protocols::Zmq2::Dealer
      attr :incoming_queue
      def initialize(opts={})
        super(opts)
        @connected = opts[:connected]
        @incoming_queue = []
      end
      def peer_free(peer_ident, connection)
        super
        @connected.succeed  if @free_peers.size == 2
      end
      def receive_message(message)
        @incoming_queue << message
      end
    end

    let(:dealer) do
      MyDealer.new(identity: 'MyDealer', connected: connected)
    end

    let(:messages) do
      10000.times.map{|n| ['', 'hello', 'world', n.to_s]}
    end

    class Collector
      attr :res_a, :res_b
      def initialize(till)
        @till = till
        @res_a, @res_b = [], []
      end
      def full?
        @res_a.size + @res_b.size >= @till
      end
      def full_res
        @res_a + @res_b
      end
      def set_sockets(zbind, zconnect)
        @zbind = zbind
        @zconnect = zconnect
      end
      def thread
        Thread.new do
          begin
          result = []
          until full?
            while @zbind.recv_strings(result, ZMQ::NOBLOCK) != -1
              result.shift.must_equal 'MyDealer'
              @res_a << result
              result = []
            end
            while @zconnect.recv_strings(result, ZMQ::NOBLOCK) != -1
              result.shift.must_equal 'MyDealer'
              @res_b << result
              result = []
            end
            sleep(0.01)
          end
          rescue
            puts $!
          end
        end
      end
    end

    it "should be able to send a lot of messages" do
      collector = Collector.new(messages.size)
      with_native do
        collector.set_sockets @zbind, @zconnect
        thrd = collector.thread
        EM.run {
          dealer.connect(ZBIND_ADDR)
          dealer.bind(ZCONNECT_ADDR)
          connected.callback {
            messages.each{|message|
              dealer.send_message(message).must_equal true
            }
            cb = lambda do
              unless collector.full?
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
      collector.full?.must_equal true
      collector.res_a.size.must_be :>, 0
      collector.res_a.size.must_be :<, messages.size
      (messages - collector.full_res).must_be_empty
      (collector.full_res - messages).must_be_empty
    end

    it "should be closed after writting" do
      collector = Collector.new(messages.size)
      with_native do
        collector.set_sockets @zbind, @zconnect
        thrd = collector.thread
        EM.run {
          dealer.connect(ZBIND_ADDR)
          dealer.bind(ZCONNECT_ADDR)
          connected.callback {
            messages.each{|message|
              dealer.send_message(message).must_equal true
            }
            dealer.close_after_writting
            EM.add_timer(0.1){ EM.next_tick{ EM.stop } }
          }
        }
        thrd.join
      end
      collector.full?.must_equal true
      collector.res_a.size.must_be :>, 0
      collector.res_a.size.must_be :<, messages.size
      (messages - collector.full_res).must_be_empty
      (collector.full_res - messages).must_be_empty
    end
  end
end
