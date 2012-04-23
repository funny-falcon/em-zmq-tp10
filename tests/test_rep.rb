require 'eventmachine'
require 'minitest/autorun'
require File.expand_path('../helper.rb', __FILE__)

require 'em/protocols/zmq2/rep'

describe 'Rep' do
  let(:connected) do
    EM::DefaultDeferrable.new
  end

  let(:finished) do
    EM::DefaultDeferrable.new
  end

  module MyRepMixin
    attr :incoming_queue
    def initialize(connected, finished, opts={})
      super opts
      @connected = connected
      @finished = finished
      @incoming_queue = []
    end
    def peer_free(peer_identity, connection)
      super
      @connected.succeed
    end
    def receive_request(message, envelope)
      message.first.must_equal 'hello'
      envelope.first.must_be :start_with?, 'BIND_'
      send_reply(['world', *message[1..-1]], envelope)
      @finished.succeed  if message.last == 'xxx'
    end
  end

  describe EM::Protocols::Zmq2::PreRep do
    class MyPreRep < EM::Protocols::Zmq2::PreRep
      include MyRepMixin
    end

    attr :req
    before do
      @req = MyPreRep.new(connected, finished, identity: 'REP')
      @req.connect(Native::ZBIND_ADDR)
    end

    let(:messages){
      300.times.map{|i| ['hello', i.to_s]} << ['hello', 'xxx']
    }

    it 'should make replies' do
      replies = []
      Native.with_socket('REQ') do |zreq|
        thrd = Thread.new do
          messages.each do |message|
            zreq.send_strings message
            reply = []
            zreq.recv_strings reply
            replies << reply
          end
        end
        EM.run {
          finished.callback {
            req.close do
              EM.next_tick{ EM.stop }
            end
          }
        }
        thrd.join
      end
      replies.all?{|reply| reply.first == 'world'}.must_equal true
      replies.map{|reply| ['hello', *reply[1..-1]]}.must_equal messages
    end
  end

  describe EM::Protocols::Zmq2::Rep do
    class MyRep < EM::Protocols::Zmq2::Rep
      include MyRepMixin
    end

    attr :req
    before do
      @req = MyRep.new(connected, finished, identity: 'REP')
      @req.connect(Native::ZBIND_ADDR)
    end

    let(:messages){
      5000.times.map{|i| ['hello', i.to_s]} << ['hello', 'xxx']
    }

    it 'should make a lot of replies' do
      replies = []
      Native.with_socket('DEALER') do |zreq|
        thrd = Thread.new do
          messages.each do |message|
            zreq.send_strings ['', *message]
          end
          messages.size.times do
            reply = []
            zreq.recv_strings reply
            replies << reply[1..-1]
          end
        end
        EM.run {
          finished.callback {
            req.close do
              EM.next_tick{ EM.stop }
            end
          }
        }
        thrd.join
      end
      replies.all?{|reply| reply.first == 'world'}.must_equal true
      replies.map{|reply| ['hello', *reply[1..-1]]}.must_equal messages
    end
  end
end
