require 'eventmachine'
require 'minitest/autorun'
require File.expand_path('../helper.rb', __FILE__)

require 'em/protocols/zmq2'

describe 'InProc' do
  let(:connected) do
    EM::DefaultDeferrable.new
  end

  class IPDealer < EM::Protocols::Zmq2::Dealer
    include SocketMixin
    def initialize(opts={})
      super(opts)
      @connected = opts[:connected]
    end
    def peers
      @peers
    end
  end

  let(:messages) do
    1000.times.map{|i| ['hi', i.to_s]}
  end

  let(:fan) do
    IPDealer.new(connected: connected, identity: 'fan')
  end

  let(:sinks) do
    2.times.map{|i| IPDealer.new(identity: "sink.#{i}") }
  end

  def run_test
    EM.run {
      connected.callback do
        fan.peers['sink.0'].wont_be_nil
        fan.peers['sink.1'].wont_be_nil
        sinks.each{|sink| sink.peers['fan'].wont_be_nil}
        messages.each{|message| fan.send_message(message)}
        fan.close do
          EM.next_tick{ EM.stop }
        end
      end
    }
    sinks.each do |sink|
      sink.incoming_queue.size.must_be :>, 0
      sink.incoming_queue.size.must_be :<, messages.size
    end
    incomings = sinks.map(&:incoming_queue).flatten(1)
    (incomings - messages).must_be_empty
    (messages - incomings).must_be_empty
  end

  it "should send messages to several connected socket" do
    sinks.each{|sink| sink.connect 'inproc://fan'}
    fan.bind 'inproc://fan'
    run_test
  end

  it "should send messages to several binded sockets" do
    sinks.each{|sink|
      bind_point = "inproc://#{sink.identity}"
      sink.bind bind_point
      fan.connect bind_point
    }
    run_test
  end

  it "should send messages to inproc and tcp" do
    sinks[0].bind 'tcp://127.0.0.1:9876'
    fan.connect 'tcp://127.0.0.1:9876'
    sinks[1].bind 'inproc://sink.1'
    fan.connect 'inproc://sink.1'
    run_test
  end

  if RUBY_PLATFORM !~ /window/
    it "should send messages to inproc and tcp" do
      sinks[0].bind 'ipc://falcon'
      fan.connect 'ipc://falcon'
      sinks[1].bind 'inproc://sink.1'
      fan.connect 'inproc://sink.1'
      run_test
    end
  end

  class IPPub < EM::Protocols::Zmq2::Pub
    include SocketMixin
  end
  class IPSub < EM::Protocols::Zmq2::Sub
    include SocketMixin
  end
  it "should pub messages to inproc and tcp sub" do
    pub = IPPub.new(identity: 'pub', connected: connected)
    subs = 2.times.map{ sub = IPSub.new(subscribe: 'hi') }
    subs[0].bind 'tcp://127.0.0.1:9876'
    pub.connect 'tcp://127.0.0.1:9876'
    subs[1].bind 'inproc://sink.1'
    pub.connect 'inproc://sink.1'
    EM.run {
      connected.callback {
        messages.each{|message| pub.send_message(message)}
        pub.close do
          EM.next_tick{ EM.stop }
        end
      }
    }
    subs.each {|sub|
      sub.incoming_queue.must_equal messages
    }
  end
end
