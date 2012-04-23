require 'eventmachine'
require 'ffi-rzmq'
require 'minitest/autorun'

require 'em/protocols/zmq2/req'

describe 'Req' do
  ZBIND_ADDR = 'tcp://127.0.0.1:7890'
  def setup_native
    @zctx = ZMQ::Context.new
    @zrep = @zctx.socket(ZMQ::REP)
    @zrep.identity = 'BIND_REP'
    @zrep.bind(ZBIND_ADDR)
  end

  def close_native
    @zrep.setsockopt(ZMQ::LINGER, 0)
    @zrep.close
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

    let(:req){ MyPreReq.new(identity: 'REQ', connected: connected)}
    let(:messages){
      ar = 300.times.map{|i| ['hello', i.to_s]}
      ar << ['hello', 'xxx']
      ar
    }

    it 'should send requests' do
      with_native do
        thrd = Thread.new do
          messages.size.times do
            ar = []
            @zrep.recv_strings ar
            ar[0] = 'world'
            @zrep.send_strings ar
            break if ar.last == 'xxx'
          end
        end
        EM.run {
          req.connect(ZBIND_ADDR)
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
        (req.incoming_queue - messages).must_be_empty
        (messages - req.incoming_queue).must_be_empty
      end
    end
  end

  describe EM::Protocols::Zmq2::Req do
    class MyReq < EM::Protocols::Zmq2::Req
      attr :incoming_queue
      def initialize(opts={}, defered_conn, defered_mess)
        super opts
        @defered_conn = defered_conn
        @defered_mess = defered_mess
        @incoming_queue = []
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
    end

    let(:finished){ EM::DefaultDeferrable.new }
    let(:req){ MyReq.new({identity: 'REQ'}, connected, finished)}
    let(:messages){
      ar = 1000.times.map{|i| ['hello', i.to_s]}
      ar << ['hello', 'xxx']
      ar
    }

    it 'should send a lot of requests' do
      with_native do
        thrd = Thread.new do
          messages.size.times do
            ar = []
            @zrep.recv_strings ar
            ar[0] = 'world'
            @zrep.send_strings ar
            break if ar.last == 'xxx'
          end
        end
        EM.run {
          req.connect(ZBIND_ADDR)
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
        (req.incoming_queue - messages).must_be_empty
        (messages - req.incoming_queue).must_be_empty
      end
    end
  end
end
