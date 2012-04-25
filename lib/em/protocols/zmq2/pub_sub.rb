require 'em/protocols/zmq2/socket'
require 'em/protocols/zmq2/queue_per_peer'
module EventMachine
  module Protocols
    module Zmq2
      # ZMQ socket which acts as SUB
      # subscriptions are done by subscribe method
      class Sub < Socket
        class DefaultAction
          def initialize(sub)
            @sub = sub
          end

          def call(message)
            @sub.receive_message(message)
          end
        end

        def initialize(opts = {})
          super
          @subscriptions = {}
          @default_action = DefaultAction.new(self)
          subscribe_many [*opts[:subscribe]]
        end

        def subscribe_many(subscriptions)
          subscriptions.each{|sub| subscribe *sub}  if subscriptions
        end

        def subscribe(s, cb = nil, &block)
          @subscriptions[s] = cb || block || @default_action
        end

        def receive_message_and_peer(message, peer)
          for sub, callback in @subscriptions
            matched = if String === sub
                        message.first.start_with?(sub)
                      elsif sub.respond_to?(:call)
                        sub.call(message.first)
                      else
                        sub === message.first  # Regexp and anything you want
                      end
            callback.call(message) if matched
          end
        end

        def receive_message(message)
          raise NoMethodError
        end

        private :send_message
      end

      class PrePub < Socket
        include PackString
        def send_message(message, even_if_busy = false)
          sent = false
          prepared = prepare_message(message)
          peers = even_if_busy ? @peers : @free_peers
          for identity, peer in peers
            if !peer.error?
              peer.send_strings_or_prepared(message, prepared)
              sent = true
            end
          end
        end
      end

      class Pub < Socket
        include QueuePerPeer
        include PackString

        def send_message(message)
          sent = false
          prepared = prepare_message(message)
          idents = @peers.keys | @queues.keys
          for identity in idents
            peer = @free_peers[identity]
            queue = @queues[identity]
            if peer && (queue.empty? || flush_queue(queue, peer)) && 
               !peer.error? && peer.not_too_busy?
              peer.send_strings_or_prepared(message, prepared)
              sent = true
            else
              pushed = push_to_queue(queue, [message, prepared])
              sent ||= pushed
            end
          end
          sent
        end
      private
        def cancel_message(message)
          false
        end

        def send_formed_message(peer, from_queue)
          peer.send_strings_or_prepared(*from_queue)
        end
      end
    end
  end
end
