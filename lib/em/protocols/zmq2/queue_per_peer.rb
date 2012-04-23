module EventMachine
  module Protocols
    module Zmq2
      module QueuePerPeer
        def initialize(opts = {})
          super
          @queues = {}
        end

        def register_peer(peer_identity, connection)
          peer_identity = super
          @queues[peer_identity] ||= []
          peer_identity
        end

        def unregister_peer(peer_identity)
          super
          if generated_identity?(peer_identity)
            @queues.delete peer_identity
          end
        end

      private
        def react_on_hwm_decrease
          @queues.each{|_, queue| push_to_queue(queue)}
        end

        def peer_free(peer, connection)
          super
          peer_conn = @peers[peer]
          queue = @queues[peer]
          flush_queue(queue, peer_conn)
        end

        def flush_queue(queue, peer, even_if_busy = false)
          until queue.empty?
            return false if peer.error? || !(even_if_busy || peer.not_too_busy?)
            peer.send_strings(form_message(queue.shift))
          end
          true
        end

        def form_message(message)
          message
        end

        def flush_all_queue
          @peers.each{|peer_identity, peer|
            if queue = @queues[peer_identity]
              flush_queue(queue, peer, true)
            end
          }
        end
      end
    end
  end
end
