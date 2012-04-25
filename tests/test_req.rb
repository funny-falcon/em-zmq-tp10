require 'eventmachine'
require 'minitest/autorun'
require File.expand_path('../helper.rb', __FILE__)

require 'em/protocols/zmq2/req'

describe 'Req' do
  let(:connected) do
    EM::DefaultDeferrable.new
  end

  describe EM::Protocols::Zmq2::PreReq do
    class MyPreReq < EM::Protocols::Zmq2::PreReq
      attr :incoming_queue
      def initialize(opts={})
        super
        @connected = opts[:connected]
        @incoming_queue = []
      end
      def peer_free(peer_identity, connection)
        super
        @connected.succeed
      end
      def receive_reply(message, data, request_id)
        message.first.must_equal 'world'
        message.last.must_equal data.to_s
        @incoming_queue << ['hello', message.last]
      end
    end

    attr :req
    before do
      @req = MyPreReq.new(identity: 'REQ', connected: connected)
      @req.connect(Native::ZBIND_ADDR)
    end

    let(:messages){
      300.times.map{|i| ['hello', i.to_s]} << ['hello', 'xxx']
    }

    it 'should send requests' do
      Native.with_socket('REP') do |zrep|
        thrd = Thread.new do
          messages.size.times do
            ar = []
            zrep.recv_strings ar
            ar[0] = 'world'
            zrep.send_strings ar
          end
        end
        EM.run {
          connected.callback {
            dup = messages.dup
            cb = lambda {
              if dup.empty?
                EM.add_timer(0.3){
                  EM.next_tick{ EM.stop }
                }
              else
                message = dup.first
                if String === req.send_request(message, message.last)
                  dup.shift
                  EM.next_tick cb
                else
                  EM.add_timer 0.1, cb
                end
              end
            }
            cb.call
          }
        }
        thrd.join
      end
      (req.incoming_queue - messages).must_be_empty
      (messages - req.incoming_queue).must_be_empty
    end
  end

  describe EM::Protocols::Zmq2::Req do
    class MyReq < EM::Protocols::Zmq2::Req
      attr :incoming_queue, :canceled_requests
      def initialize(opts={}, defered_conn, defered_mess)
        super opts
        @defered_conn = defered_conn
        @defered_mess = defered_mess
        @incoming_queue = []
        @canceled_requests = []
      end
      def peer_free(peer_identity, connection)
        super
        @defered_conn.succeed
      end
      def receive_reply(message, data, request_id)
        message.first.must_equal 'world'
        message.last.must_equal data.to_s
        @incoming_queue << ['hello', message.last]
        @defered_mess.succeed  if message.last == 'xxx'
      end
      def cancel_request(request_id)
        @canceled_requests << request_id
      end
    end

    let(:finished){ EM::DefaultDeferrable.new }
    attr :req
    before do
      @req = MyReq.new({identity: 'REQ'}, connected, finished)
      @req.connect(Native::ZBIND_ADDR)
    end
    let(:messages){
      5000.times.map{|i| ['hello', i.to_s]} << ['hello', 'xxx']
    }

    it 'should send a lot of requests' do
      Native.with_socket('REP') do |zrep|
        thrd = Thread.new do
          messages.size.times do
            ar = []
            zrep.recv_strings ar
            ar[0] = 'world'
            zrep.send_strings ar
          end
        end
        EM.run {
          connected.callback {
            messages.each{|message|
              req.send_request(message, message.last)
            }
          }
          finished.callback {
            EM.next_tick{ EM.stop }
          }
        }
        thrd.join
      end
      (req.incoming_queue - messages).must_be_empty
      (messages - req.incoming_queue).must_be_empty
    end

    it "should not accept message on low hwm with strategy :drop_last" do
      req.hwm = 2
      req.hwm_strategy = :drop_last
      EM.run {
        req.send_request(['hi', 'ho1'], nil).must_be_kind_of String
        req.send_request(['hi', 'ho2'], nil).must_be_kind_of String
        req.send_request(['hi', 'ho3'], nil).wont_be_kind_of String
        EM.stop
      }
    end

    it "should cancel earlier message on low hwm with strategy :drop_first" do
      req.hwm = 2
      req.hwm_strategy = :drop_first
      first_req = nil
      EM.run {
        first_req = req.send_request(['hi', 'ho1'], nil)
        first_req.must_be_kind_of String
        req.send_request(['hi', 'ho2'], nil).must_be_kind_of String
        req.send_request(['hi', 'ho3'], nil).must_be_kind_of String
        EM.stop
      }
      req.canceled_requests.must_equal [first_req]
    end
  end

  describe EM::Protocols::Zmq2::ReqDefer do
    class TSTRep < EM::Protocols::Zmq2::Rep
      include DeferredMixin
      def receive_request(message, environment)
        send_reply([message.first, 'yeah'], environment)
      end
      def peer_free(peer, conn)
        super
        @connected.succeed
      end
    end
    it "should correctly setup and call deferrable" do
      req = EM::Protocols::Zmq2::ReqDefer.new
      def req.send_message(message, even = false)
        res = super
        res
      end
      rep = TSTRep.new(connected: connected)
      rep.bind('inproc://tst')
      first_success, first_error = nil, nil
      second_success, second_error = nil, nil
          require 'pp'
      EM.run {
        uniq = Object.new.freeze
        first = nil
        follow = proc do |null, data|
          data.must_equal uniq
          null.must_be_nil
          req.connect('inproc://tst')
          connected.callback do
            stop = proc{ EM.next_tick{ EM.stop } }
            second = req.send_request(['hi', 'ho'], uniq) do |reply, data|
              second_success = true
              reply.must_equal ['hi', 'yeah']
              data.must_equal uniq
            end
            second.must_be_kind_of EM::Deferrable
            second.errback do
              second_error = true
            end
            second.callback &stop
            second.errback &stop
          end
        end
        first = req.send_request(['hi', 'ho'], uniq) do |reply, data|
          first_success = true
          reply.must_equal ['hi', 'yeah']
          data.must_equal uniq
        end
        first.must_be_kind_of EM::Deferrable
        first.timeout(0.3, 1, uniq)
        first.errback do
          first_error = true
        end
        first.callback &follow
        first.errback &follow
      }
      first_success.must_equal nil
      first_error.must_equal true
      second_success.must_equal true
      second_error.must_equal nil
    end
  end
end
