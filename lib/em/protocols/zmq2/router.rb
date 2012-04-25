require 'em/protocols/zmq2/socket'
require 'em/protocols/zmq2/queue_per_peer'
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

        def send_message(message, even_if_busy = false)
          if connect = choose_peer(message.first, even_if_busy)
            connect.send_strings(message[1..-1])
            true
          end
        end

      private
        # by default chooses peer by peer_identity, but you could wary it
        def choose_peer(peer_identity, even_if_busy = false)
          if (connect = @peers[peer_identity]) && !connect.error? &&
             (even_if_busy || connect.not_too_busy?)
            connect
          end
        end

      end

      # ZMQ socket which acts like Router.
      # It counts first message string as peer identity when sending message and
      # prepends socket identity to message on receiving.
      class Router < PreRouter
        include QueuePerPeer
        def send_message(message)
          peer_identity = message.first
          unless (queue = @queues[peer_identity])
            if generated_identity?(peer_identity)
              return false
            else
              queue = @queues[peer_identity] = []
            end
          end
          peer = choose_peer(peer_identity)
          if peer && (queue.empty? || flush_queue(queue, peer)) &&
             !peer.error? && peer.not_too_busy?
            peer.send_strings(message[1..-1])
            true
          else
            push_to_queue(queue, message)
          end
        end

      private
        def send_formed_message(peer, from_queue)
          peer.send_strings(from_queue[1..-1])
        end
      end

    end
  end
end
