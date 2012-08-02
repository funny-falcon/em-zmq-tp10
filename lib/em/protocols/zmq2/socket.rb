require 'em/protocols/zmq2/socket_connection'
require 'em/protocols/zmq2/inproc'
module EM
  module Protocols
    module Zmq2
      # Base class for all ZMQ sockets.
      # It implements address parsing, binding, connecting and reconnecting
      # For implementing your own kind of socket you should override at least
      # #receive_message, and define method which will choose peer from @peers,
      # and call its peer#send_strings
      class Socket
        attr :identity, :hwm
        attr_accessor :hwm_strategy
        attr_accessor :do_balance
        GENERATED = '%GN%'.freeze

        # Accept options, which are dependend on socket type
        # Common options are:
        #  [+:identity+] socket identity
        #  [+:hwm+]      highwater mark
        #  [+:hwm_strategy+]  what to do on send_message when :hwm reached, hwm_strategy could be:
        #                     +:drop_last+ - do not accept message for sending - what Zmq does
        #                     +:drop_first+ - remove message from queue head and add this message to queue tail
        #
        # this class provides convinient method +#push_to_queue+ for default strategy, but
        # it is up to subclass how to use it.
        #
        # Another note concerning highwatermark: EventMachine does not allow precise control on
        # outgoing data buffer, so that there is a bit more message will be lost, when outgoing
        # peer disconnected.
        def initialize(opts = {})
          @hwm = opts[:hwm] || HWM_INFINITY
          @hwm_strategy = opts[:hwm_strategy] || :drop_last
          @identity = opts[:identity] || EMPTY
          @do_balance = opts.fetch(:do_balance, true)
          @peers = {}
          @free_peers = {}
          @connections = {}
          @conn_addresses = {}
          @reconnect_timers = {}
          @bindings = []
          @after_writting = nil
          @uniq_identity = '%GN%aaaaaaaaaaaa' # ~ 100 years to overflow
        end

        # binding to port
        # :call-seq:
        #   bind('tcp://host:port') - bind to tcp port
        #   bind('ipc://filename') - bind to unix port
        def bind(addr)
          kind, *socket = parse_address(addr)
          EM.schedule {
            @bindings << case kind
                when :tcp, :ipc
                  EM.start_server(*socket, SocketConnection, self)
                when :inproc
                  InProc.bind(addr, self)
                end
          }
        end

        # connect to port
        # :call-seq:
        #   connect('tcp://host:port') - connect to tcp port
        #   connect('ipc://filename') - connect to unix port
        def connect(addr)
          kind, *socket = parse_address(addr)
          EM.schedule lambda{
            @reconnect_timers.delete addr
            unless @conn_addresses[ addr ]
              connection = case kind
                  when :tcp
                    EM.connect(*socket, SocketConnection, self)
                  when :ipc
                    begin
                      EM.connect_unix_domain(*socket, SocketConnection, self)
                    rescue RuntimeError
                      timer = EM.add_timer(SMALL_TIMEOUT) do
                        connect(addr)
                      end
                      @reconnect_timers[addr] = timer
                      break
                    end
                  when :inproc
                    InProc.connect(addr, self)
                  end
              @connections[ connection ] = addr
              @conn_addresses[ addr ] = connection
            end
          }
        end

        # :stopdoc:
        def not_connected(connection)
          if addr = @connections.delete(connection)
            @conn_addresses.delete addr
            timer = EM.add_timer(SMALL_TIMEOUT) do
              connect(addr)
            end
            @reconnect_timers[addr] = timer
          end
        end

        def register_peer(peer_identity, connection)
          peer_identity = next_uniq_identity  if peer_identity.empty?
          @peers[peer_identity] = connection
          EM.next_tick{ peer_free(peer_identity, connection) }
          peer_identity
        end

        def unregister_peer(peer_identity)
          @peers.delete peer_identity
          @free_peers.delete peer_identity
          if @peers.empty? && @after_writting.respond_to?(:call)
            @after_writting.call
          end
        end

        # :startdoc:

        # close all connections
        # if callback is passed, then it will be called after all messages written to sockets
        def close(cb = nil, &block)
          @connections.clear
          @conn_addresses.clear
          @reconnect_timers.each{|_, timer| EM.cancel_timer(timer)}
          @after_writting = cb || block
          flush_all_queue  if @after_writting
          @peers.values.each{|c| c.close_connection(!!@after_writting)}
          @bindings.each{|c|
            unless String === c
              EM.stop_server c
            else
              InProc.unbind(c, self)
            end
          }
        end

        # override to make sure all messages are sent before socket closed
        def flush_all_queue
          true
        end

        # stub method for sending message to a socket
        # note that every socket type should define proper behaviour here
        # or/and define another useful, semantic clear methods
        def send_message(message)
          raise NoMethodError
        end

        # callback method called with underlied connection when
        # some message arrives
        def receive_message_and_peer(message, peer_identity)
          raise NoMethodError
        end

        # Change hwm
        def hwm=(new_hwm)
          old_hwm, @hwm = @hwm, new_hwm
          react_on_hwm_decrease if old_hwm > @hwm
          @hwm
        end

        # callback method, called when underlying peer is free for writing in
        # should be used in subclasses for proper reaction on network instability
        def peer_free(peer_identity, peer) # :doc:
          @free_peers[peer_identity] = peer
        end

      private
        # splits message into envelope and message as defined by ZMQ 2.x
        def split_message(message) # :doc:
          i = message.index(EMPTY)
          [message.slice(0, i), message.slice(i+1, message.size)]
        end

        # helper method for managing queue concerning @hwm setting
        def push_to_queue(queue, message = nil) # :doc
          if queue.size >= @hwm
            case @hwm_strategy
            when :drop_last
              if queue.size > @hwm
                queue.pop(queue.size - @hwm).each{|message|
                  cancel_message(message)
                }
              end
              false
            when :drop_first
              hwm = @hwm - (message ? 1 : 0)
              queue.shift(queue.size - hwm).each{|earlier|
                cancel_message(earlier)
              }
              queue.push(message.dup)  if message
              true
            end
          else
            queue.push(message.dup) if message
            true
          end
        end

        # override to correctly react on hwm decrease
        def react_on_hwm_decrease # :doc:
          true
        end

        # overried if you should react on dropped requests
        def cancel_message(message) # :doc:
          true
        end

        def next_uniq_identity
          res = @uniq_identity
          @uniq_identity = res.next
          res
        end

        def generated_identity?(id)
          id.start_with?(GENERATED)
        end

        def parse_address(addr)
          case addr
          when %r{tcp://([^:]+):(\d+)}
            [:tcp, $1, $2.to_i]
          when %r{ipc://(.+)}
            [:ipc, $1]
          when %r{inproc://(.+)}
            [:inproc, $1]
          else
            raise 'Not supported ZMQ socket kind'
          end
        end

      end
    end
  end
end
