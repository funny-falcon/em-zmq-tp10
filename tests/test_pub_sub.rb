require 'eventmachine'
require 'minitest/autorun'
require File.expand_path('../helper.rb', __FILE__)

require 'em/protocols/zmq2/pub_sub'

describe 'Sub' do
  let(:connected) do
    EM::DefaultDeferrable.new
  end

  let(:finished) do
    EM::DefaultDeferrable.new
  end

  describe EM::Protocols::Zmq2::Sub do
    class MySub < EM::Protocols::Zmq2::Sub
      attr :incoming_queue
      def initialize(connected, finished, opts={})
        super opts
        @connected = connected
        @finished = finished
        @incoming_queue = []
        @to_catch = 2
      end
      def peer_free(peer, connection)
        super
        @connected.succeed  if @peers.size == 2
      end
      def receive_message(message)
        @incoming_queue << message
        if message.last == 'xxx'
          @to_catch -= 1
          @finished.succeed  if @to_catch == 0
        end
      end
    end

    attr :sub
    before do
      @sub = MySub.new(connected, finished, subscribe: 'mess')
      @sub.connect(Native::ZBIND_ADDR)
      @sub.bind(Native::ZCONNECT_ADDR)
    end

    let(:messages_a) do
      1000.times.map{|n| ['mess_a', n.to_s]} << ['mess_a', 'xxx']
    end

    let(:messages_b) do
      1000.times.map{|n| ['mess_b', n.to_s]} << ['mess_b', 'xxx']
    end

    it "should accept messages" do
      Native.with_socket_pair('PUB') do |zbind, zconnect|
        thrd1, thrd2 = nil
        EM.run do
          connected.callback do
            thrd1 = Thread.new do
              messages_a.each do |message|
                zbind.send_strings(message)
              end
            end
            thrd2 = Thread.new do
              messages_b.each do |message|
                zconnect.send_strings(message)
              end
            end
          end
          finished.callback do
            EM.next_tick{ EM.stop }
          end
        end
        thrd1.join
        thrd2.join
      end
      (sub.incoming_queue - messages_a - messages_b).must_be_empty
      (messages_a - sub.incoming_queue).must_be_empty
      (messages_b - sub.incoming_queue).must_be_empty
    end
  end

  describe EM::Protocols::Zmq2::PrePub do
    class MyPrePub < EM::Protocols::Zmq2::PrePub
      def initialize(connected, opts={})
        super opts
        @connected = connected
      end
      def peer_free(peer, connection)
        super
        @connected.succeed  if @peers.size == 2
      end
    end

    attr :pub
    before do
      @pub = MyPrePub.new(connected)
      @pub.connect(Native::ZBIND_ADDR)
      @pub.bind(Native::ZCONNECT_ADDR)
    end

    let(:messages) {
      100.times.map{|n| ['mess_a', n.to_s]} << ['mess_a', 'xxx']
    }

    it "should send messages" do
      results = []
      Native.with_socket_pair('SUB') do |zbind, zconnect|
        zbind.setsockopt(ZMQ::SUBSCRIBE, 'mess')
        zconnect.setsockopt(ZMQ::SUBSCRIBE, 'mess')
        thrds = [zbind, zconnect].map{|z| Thread.new do
          res = []
          while true
            recv = []
            z.recv_strings recv
            res << recv
            break  if recv.last == 'xxx'
          end
          res
        end}
        EM.run do
          connected.callback do
            messages.each do |message|
              pub.send_message message
            end
            pub.close do
              EM.next_tick do EM.stop end
            end
          end
        end
        results = thrds.map(&:value)
      end
      results[0].must_equal messages
      results[1].must_equal messages
    end
  end

  describe EM::Protocols::Zmq2::Pub do
    class MyPub < EM::Protocols::Zmq2::Pub
      def initialize(connected, opts={})
        super opts
        @connected = connected
      end
      def peer_free(peer, connection)
        super
        @connected.succeed  if @peers.size == 2
      end
    end

    attr :pub
    before do
      @pub = MyPub.new(connected)
      @pub.connect(Native::ZBIND_ADDR)
      @pub.bind(Native::ZCONNECT_ADDR)
    end

    let(:messages) {
      500.times.map{|n| ['mess_a', n.to_s]} << ['mess_a', 'xxx']
    }

    it "should send a lot of messages" do
      results = []
      Native.with_socket_pair('SUB') do |zbind, zconnect|
        zbind.setsockopt(ZMQ::SUBSCRIBE, 'mess')
        zconnect.setsockopt(ZMQ::SUBSCRIBE, 'mess')
        thrds = [zbind, zconnect].map{|z| Thread.new do
          res = []
          while true
            recv = []
            z.recv_strings recv
            res << recv
            #p [z.object_id, *recv]
            break  if recv.last == 'xxx'
          end
          res
        end}
        EM.run do
          connected.callback do
            messages.each do |message|
              pub.send_message message
            end
            pub.close do
              EM.next_tick do EM.stop end
            end
          end
        end
        results = thrds.map(&:value)
      end
      results[0].must_equal messages
      results[1].must_equal messages
    end

    it "should not accept message when hwm is low and strategy is :drop_last" do
      pub.hwm = 1
      pub.hwm_strategy = :drop_last
      results = nil
      EM.run do
        EM.defer proc{
          Native.with_socket_pair('SUB') do |zbind, zconnect|
            sleep 0.2
          end
        }, proc {
          pub.send_message(messages[0]).must_equal true
          pub.send_message(messages[1]).must_equal false
          connected.instance_variable_set(:@deferred_status, nil)
          connected.callback do
            pub.send_message(messages.last).must_equal true
          end
          EM.defer proc{
            Native.with_socket_pair('SUB') do |zbind, zconnect|
              zbind.setsockopt(ZMQ::SUBSCRIBE, 'mess')
              zconnect.setsockopt(ZMQ::SUBSCRIBE, 'mess')
              results = [zbind, zconnect].map{|z| Thread.new do
                res = []
                2.times do
                  recv = []
                  z.recv_strings recv
                  res << recv
                end
                res
              end}.map(&:value)
            end
          }, proc{
            EM.next_tick{ EM.stop }
          }
        }
      end
      results[0].must_equal [messages.first, messages.last]
      results[1].must_equal [messages.first, messages.last]
    end

    it "should drop first message when hwm is low and strategy is :drop_first" do
      pub.hwm = 1
      pub.hwm_strategy = :drop_first
      results = nil
      EM.run do
        EM.defer proc{
          Native.with_socket_pair('SUB') do |zbind, zconnect|
            sleep 0.2
          end
        }, proc {
          pub.send_message(messages[0]).must_equal true
          pub.send_message(messages[1]).must_equal true
          connected.instance_variable_set(:@deferred_status, nil)
          connected.callback do
            pub.send_message(messages.last).must_equal true
          end
          EM.defer proc{
            Native.with_socket_pair('SUB') do |zbind, zconnect|
              zbind.setsockopt(ZMQ::SUBSCRIBE, 'mess')
              zconnect.setsockopt(ZMQ::SUBSCRIBE, 'mess')
              results = [zbind, zconnect].map{|z| Thread.new do
                res = []
                2.times do
                  recv = []
                  z.recv_strings recv
                  res << recv
                end
                res
              end}.map(&:value)
            end
          }, proc{
            EM.next_tick{ EM.stop }
          }
        }
      end
      results[0].must_equal [messages[1], messages.last]
      results[1].must_equal [messages[1], messages.last]
    end
  end
end
