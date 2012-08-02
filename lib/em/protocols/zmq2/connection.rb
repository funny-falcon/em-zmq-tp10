module EventMachine
  module Protocols
    module Zmq2
      module ConnectionMixin
        # use watching on outbound queue when possible
        # rely on https://github.com/eventmachine/eventmachine/pull/317 if were accepted
        # or on https://github.com/funny-falcon/eventmachine/tree/sent_data
        # use timers otherwise
        def sent_data
          @socket.peer_free(@peer_identity, self) if not_too_busy?
        end

        def not_too_busy?
          !error? && (!@socket.do_balance || begin
            free = _not_too_busy?
            self.notify_when_free = !free
            free
          end)
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
          self.notify_when_free= false
          if @peer_identity
            @socket.unregister_peer(@peer_identity)
          end
          @socket.not_connected(self)
        end
      end
    end
  end
end
