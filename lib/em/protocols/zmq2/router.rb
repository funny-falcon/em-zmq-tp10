require 'em/protocols/zmq2/socket'
module EventMachine
  module Protocols
    module Zmq2
      # ZMQ socket which acts like Router but without outgoing message queueing.
      # It counts first message string as peer identity when sending message and
      # prepends socket identity to message on receiving.
      class PreRouter < Socket
        def receive_message_and_peer(message, peer_identity) # :nodoc:
          message.unshift(peer_identity)
          receive_message(message)
        end

        def receive_message(message)
          raise NoMethodError
        end

        # by default chooses peer by peer_identity, but you could wary it
        def choose_peer(peer_identity, even_if_busy = false)
          if (connect = @free_peers[peer_identity]) && !connect.error? &&
             (even_if_busy || connect.not_too_busy?)
            connect
          end
        end

        def send_message(message, even_if_busy = false)
          if connect = choose_peer(message.first, even_if_busy)
            connect.send_strings(message[1..-1])
            true
          end
        end
      end

      # ZMQ socket which acts like Router.
      # It counts first message string as peer identity when sending message and
      # prepends socket identity to message on receiving.
      class Router < PreRouter
        def initialize(opts = {})
          super
          @replies = {}
        end

        def flush_queue(peer_identity, even_if_busy = false)
          return true  unless peer_queue = @replies[peer_identity]
          until peer_queue.empty?
            message = peer_queue.first
            if (connect = choose_peer(peer_identity, even_if_busy)) && 
                connect.send_strings(peer_queue.first)
              peer_queue.shift
            else
              return false
            end
          end
          @replies.delete peer_identity
          true
        end

        def send_message(message)
          peer_identity = message.first
          flush_queue(peer_identity) && super(message) || begin 
            unless generated_idenity?(peer_identity)
              push_to_queue(@replies[peer_identity], message)
            end
          end
        end

        def flush_all_queue
          @replies.keys.each{|peer_identity| flush_queue(peer_identity, true)}
        end

        def react_on_hwm_decrease
          @replies.each{|_, queue| push_to_queue(queue)}
        end
      private
        def peer_free(peer, connection)
          super
          flush_queue(peer)
        end
      end

    end
  end
end
