require 'eventmachine'
require 'minitest/autorun'
require File.expand_path('../helper.rb', __FILE__)

require 'em/protocols/zmq2/router'

describe 'Router' do

  let(:connected) do
    EM::DefaultDeferrable.new
  end

  let(:finished) do
    EM::DefaultDeferrable.new
  end

  class DealerCollector
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
            result.first.must_equal 'hello'
            @res_a << result
            result = []
          end
          while @zconnect.recv_strings(result, ZMQ::NOBLOCK) != -1
            result.first.must_equal 'hello'
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

  describe EM::Protocols::Zmq2::PreRouter do
    class MyPreRouter < EM::Protocols::Zmq2::PreRouter
      attr :incoming_queue
      def initialize(connected, finished, opts={})
        super opts
        @connected = connected
        @finished = finished
        @incoming_queue = {}
      end
      def peer_free(peer_ident, connection)
        super
        @connected.succeed  if @free_peers.size == 2
      end
      def receive_message(message)
        (@incoming_queue[message.first] ||= []) << message[1..-1]
        @finished.succeed  if message.last == 'xxx'
      end
    end

    attr :router
    before do
      @router = MyPreRouter.new(connected, finished, identity: 'MyRouter')
      @router.bind(Native::ZCONNECT_ADDR)
      @router.connect(Native::ZBIND_ADDR)
    end

    let(:messages) do
      62.times.map{|n| ['hello', n.to_s] } << ['hello', 'xxx']
    end

    it "should be able to receive messages" do
      halves = messages[0...(messages.size/2)], messages[(messages.size/2)..-1]
      Native.with_socket_pair('DEALER') do |zbind, zconnect|
        EM.run {
          connected.callback do
            halves[0].each do |message|
              zbind.send_strings(message)
            end
            halves[1].each do |message|
              zconnect.send_strings(message)
            end
          end
          finished.callback do
            EM.next_tick{ EM.stop }
          end
        }
      end
      router.incoming_queue['BIND_DEALER'].must_equal halves[0]
      router.incoming_queue['CONNECT_DEALER'].must_equal halves[1]
    end

    it "should be able to route messages" do
      collector = DealerCollector.new(messages.size)
      halves = messages[0...(messages.size/2)], messages[(messages.size/2)..-1]
      Native.with_socket_pair('DEALER') do |zbind, zconnect|
        collector.set_sockets zbind, zconnect
        thrd = collector.thread
        EM.run {
          dup_halves = halves.map(&:dup)
          connected.callback do
            dup_halves[0].each do |message|
              router.send_message(['BIND_DEALER', *message]).must_equal true
            end
            dup_halves[1].each do |message|
              router.send_message(['CONNECT_DEALER', *message]).must_equal true
            end
            EM.defer(proc do thrd.join end, proc do
              EM.next_tick{ EM.stop }
            end)
          end
        }
      end
      collector.res_a.must_equal halves[0]
      collector.res_b.must_equal halves[1]
    end
  end

  describe EM::Protocols::Zmq2::Router do
    class MyRouter < EM::Protocols::Zmq2::Router
      attr :incoming_queue, :canceled_messages
      def initialize(connected, finished, opts={})
        super opts
        @connected = connected
        @finished = finished
        @incoming_queue = {}
        @canceled_messages = []
      end
      def peer_free(peer_ident, connection)
        super
        @connected.succeed  if @free_peers.size == 2
      end
      def receive_message(message)
        (@incoming_queue[message.first] ||= []) << message[1..-1]
        @finished.succeed  if message.last == 'xxx'
      end
      def cancel_message(message)
        @canceled_messages << message
      end
    end

    attr :router
    before do
      @router = MyRouter.new(connected, finished, identity: 'MyRouter')
      @router.bind(Native::ZCONNECT_ADDR)
      @router.connect(Native::ZBIND_ADDR)
    end

    let(:messages) do
      10000.times.map{|n| ['hello', n.to_s] } << ['hello', 'xxx']
    end

    it "should be able to route a lot of messages" do
      collector = DealerCollector.new(messages.size)
      halves = messages[0...(messages.size/2)], messages[(messages.size/2)..-1]
      Native.with_socket_pair('DEALER') do |zbind, zconnect|
        collector.set_sockets zbind, zconnect
        thrd = collector.thread
        EM.run {
          dup_halves = halves.map(&:dup)
          connected.callback do
            dup_halves[0].each do |message|
              router.send_message(['BIND_DEALER', *message])
            end
            dup_halves[1].each do |message|
              router.send_message(['CONNECT_DEALER', *message])
            end
            EM.defer(proc do thrd.join end, proc do
              EM.next_tick{ EM.stop }
            end)
          end
        }
        thrd.join
      end
      collector.res_a.must_equal halves[0]
      collector.res_b.must_equal halves[1]
    end

    it "should not accept message on low hwm with strategy :drop_last" do
      router.hwm = 1
      router.hwm_strategy = :drop_last
      EM.run {
        router.send_message(['FIRST_PEER', 'hi', 'ho1']).must_equal true
        router.send_message(['FIRST_PEER', 'hi', 'ho2']).wont_equal true
        router.send_message(['SECOND_PEER', 'hi', 'ho1']).must_equal true
        router.send_message(['SECOND_PEER', 'hi', 'ho2']).wont_equal true
        EM.stop
      }
    end

    it "should cancel earlier message on low hwm with strategy :drop_first" do
      router.hwm = 1
      router.hwm_strategy = :drop_first
      EM.run {
        router.send_message(['FIRST_PEER', 'hi', 'ho1']).must_equal true
        router.send_message(['FIRST_PEER', 'hi', 'ho2']).must_equal true
        router.send_message(['SECOND_PEER', 'hi', 'ho1']).must_equal true
        router.send_message(['SECOND_PEER', 'hi', 'ho2']).must_equal true
        EM.stop
      }
      router.canceled_messages.count.must_equal 2
      router.canceled_messages.must_include ['FIRST_PEER', 'hi', 'ho1']
      router.canceled_messages.must_include ['SECOND_PEER', 'hi', 'ho1']
    end
  end
end
