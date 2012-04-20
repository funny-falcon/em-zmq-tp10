require 'em/protocols/zmq2/socket'
module EvenMachine
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
            @sub.recieve_request(message)
          end
        end

        def initialize(opts = {})
          super
          @subscriptions = {}
          @default_action = DefaultAction.new(self)
          subscrive_many opts[:subscribe]
        end

        def subscribe_many(subscriptions)
          subscriptions.each{|sub| subscribe *sub}  if subscriptions
        end

        def subscribe(s, cb = nil, &block)
          @subscriptions[s] = cb || block || @default_action
        end

        def recieve_message(message)
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

        def recieve_request(message)
          raise NoMethodError
        end
      end

      class Pub < Socket
        def initialize(opts = {})
          super
          @queues = {}
        end

        def flush_queue(peer_identity)
          return  true  unless queue = @queues[peer_identity]
          return  false unless peer = @peers[peer_identity]

          until @queues.empty?
            return false  if peer.error?
            peer.send_strings(@queues.shift)
          end
          @queues.delete(peer_identity)
          true
        end

        def send_message(message)
          for identity, peer in @peers
            if flush_queue(identity) && !peer.error?
              peer.send_strings(message)
            else
              (@queues[identity] ||= []) << message
            end
          end
        end
      end
    end
  end
end
