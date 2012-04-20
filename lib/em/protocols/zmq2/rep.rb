require 'em/protocols/zmq2/router'

module EventMachine
  module Protocols
    module Zmq2
      # mixin which transforms +PreRouter+ to +PreRep+ and +Router+ to +Rep+
      module RepMixin
        # you should not override +#recieve_message+ in +PreRep+ and +Rep+
        # see +#recieve_request+ instead
        def receive_message(message)
          envelope, message = split_message(message)
          receive_request(message, envelope)
        end

        # callback on incoming message, splits message and envelope
        # use envelope later in +#send_reply+
        def receive_request(message, envelope)
          raise NoMethodError
        end

        # joins message and envelope into single message and sends it
        def send_reply(message, envelope)
          send_message([*envelope, EMPTY, *message])
        end
      end

      # ZMQ socket which acts like REP (without outgoing queue)
      #
      # @example
      # class EchoBangRep < EM::Protocols::Zmq2::PreRep
      #   def recieve_request(message, envelope)
      #     message << "!"
      #     if send_reply(message, envelope)
      #       puts "reply sent successfuly"
      #     end
      #   end
      # end
      # rep = EchoBangRep.new
      # rep.bind('ipc://rep')
      class PreRep
        include PreRepMixin
      end

      # ZMQ socket which acts like REP
      #
      # @example
      # class EchoBangRep < EM::Protocols::Zmq2::Rep
      #   def recieve_request(message, envelope)
      #     message << "!"
      #     if send_reply(message, envelope)
      #       puts "reply sent successfuly (or placed into queue)"
      #     end
      #   end
      # end
      # rep = EchoBangRep.new
      # rep.bind('ipc://rep')
      class Rep < Router
        include PreRepMixin
      end

    end
  end
end
