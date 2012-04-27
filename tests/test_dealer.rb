require 'eventmachine'
require 'minitest/autorun'
require File.expand_path('../helper.rb', __FILE__)

require 'em/protocols/zmq2/dealer'

describe 'Dealer' do

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

    attr :dealer
    before do
      @dealer = MyPreDealer.new(identity: 'MyDealer', connected: connected)
      @dealer.connect(Native::ZBIND_ADDR)
      @dealer.bind(Native::ZCONNECT_ADDR)
    end

    let(:messages) do
      100.times.map{|n| ['', 'hello', 'world', n.to_s]}
    end

    it "should be able to send message" do
      results = []
      Native.with_socket_pair('ROUTER') do |zbind, zconnect|
        EM.run {
          connected.callback {
            messages.each{|message|
              dealer.send_message(message)
            }
            EM.add_timer(0.1){
              EM.next_tick{ EM.stop }
            }
          }
        }
        result = []
        while zbind.recv_strings(result, ZMQ::NOBLOCK) != -1
          result.shift.must_equal 'MyDealer'
          results << result
          result = []
        end
        results.size.must_be :>, 0
        results.size.must_be :<, messages.size
        while zconnect.recv_strings(result, ZMQ::NOBLOCK) != -1
          result.shift.must_equal 'MyDealer'
          results << result
          result = []
        end
      end
      (messages - results).must_be_empty
      (results - messages).must_be_empty
    end

    it "should be able to receive messages" do
      Native.with_socket_pair('ROUTER') do |zbind, zconnect|
        EM.run {
          connected.callback {
            messages[0...(messages.size/2)].each{|message|
              zbind.send_strings ['MyDealer', *message]
            }
            messages[(messages.size/2)..-1].each{|message|
              zconnect.send_strings ['MyDealer', *message]
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
  end

  describe EM::Protocols::Zmq2::Dealer do
    class MyDealer < EM::Protocols::Zmq2::Dealer
      attr :incoming_queue, :canceled_messages
      def initialize(opts={})
        super(opts)
        @connected = opts[:connected]
        @incoming_queue = []
        @canceled_messages = []
      end
      def peer_free(peer_ident, connection)
        super
        @connected.succeed  if @free_peers.size == 2
      end
      def receive_message(message)
        @incoming_queue << message
      end
      def cancel_message(message)
        @canceled_messages << message
      end
    end

    attr :dealer
    before do
      @dealer = MyDealer.new(identity: 'MyDealer', connected: connected)
      @dealer.connect(Native::ZBIND_ADDR)
      @dealer.bind(Native::ZCONNECT_ADDR)
    end

    let(:messages) do
      10000.times.map{|n| ['', 'hello', 'world', n.to_s]}
    end

    class RouteCollector
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
        end
      end
    end

    it "should be able to send a lot of messages" do
      collector = RouteCollector.new(messages.size)
      Native.with_socket_pair('ROUTER') do |zbind, zconnect|
        collector.set_sockets zbind, zconnect
        thrd = collector.thread
        EM.run {
          connected.callback {
            messages.each{|message|
              dealer.send_message(message)
            }
            dealer.close do
              EM.next_tick{ EM.stop }
            end
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
      collector = RouteCollector.new(messages.size)
      Native.with_socket_pair('ROUTER') do |zbind, zconnect|
        collector.set_sockets zbind, zconnect
        thrd = collector.thread
        EM.run {
          connected.callback {
            messages.each{|message|
              dealer.send_message(message)
            }
            dealer.close do
              EM.next_tick{ EM.stop }
            end
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

    it "should not accept message on low hwm with strategy :drop_last" do
      dealer.hwm = 1
      dealer.hwm_strategy = :drop_last
      EM.run {
        dealer.send_message(['hi', 'ho1']).must_equal true
        dealer.send_message(['hi', 'ho2']).wont_equal true
        EM.stop
      }
    end

    it "should cancel earlier message on low hwm with strategy :drop_first" do
      dealer.hwm = 1
      dealer.hwm_strategy = :drop_first
      EM.run {
        dealer.send_message(['hi', 'ho1']).must_equal true
        dealer.send_message(['hi', 'ho2']).must_equal true
        EM.stop
      }
      dealer.canceled_messages.must_equal [['hi', 'ho1']]
    end
  end
end
