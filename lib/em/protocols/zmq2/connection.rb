module EventMachine
  module Protocols
    module Zmq2
      module ConnectionMixin
        def sent_data
          @socket.peer_free(@peer_identity, self) if not_too_busy?
        end

        def not_too_busy?
          free = _not_too_busy?
          self.notify_when_free = !free
          free
        end

        def post_init
          send_strings @socket.identity
        end

        def receive_strings(message)
          unless @peer_identity
            peer_identity = message.first
            @peer_identity = @socket.register_peer(peer_identity, self)
          else
            @socket.receive_message_and_peer message, @peer_identity
          end
        end

        def unbind
          if @peer_identity
            @socket.unregister_peer(@peer_identity)
          end
          @socket.not_connected(self)
        end
      end
    end
  end
end
