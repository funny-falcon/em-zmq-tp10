require 'em/protocols/zmq2/socket'
module EventMachine
  module Protocols
    module Zmq2
      # ZMQ socket which acts like a Dealer, but without any message queueing
      # (except for EventMachine write buffers). So that, it is up to you
      # to react when message could not be send
      #
      # @example
      # class MyDealer < EM::Protocols::Zmq2::PreDealer
      #   def receive_message(message)
      #     puts "Message received: #{message.inspect}"
      #   end
      # end
      # dealer = MyDealer.new
      # dealer.connect('tcp://127.0.0.1:8000')
      # if !dealer.send_message(['asdf','fdas'])
      #   puts "Could not send message (no free peers)"
      # end
      class PreDealer < Socket
        # @private
        def choose_peer(even_if_busy = false)
          peers = even_if_busy ? @peers : @free_peers
          i = peers.size
          while i > 0
            ident, connect = peers.shift
            if even_if_busy || connect.not_too_busy?
              peers[ident] = connect # use the fact, that hash is ordered in Ruby 1.9
              return connect  unless connect.error?
            end
            i -= 1
          end
        end

        # overrides default callback to call +#receive_message+
        def receive_message_and_peer(message, peer_identity) # :nodoc:
          receive_message(message)
        end

        # override to have desired reaction on message
        def receive_message(message)
          raise NoMethodError
        end

        # tries to send message.
        # @param [Array] message - message to be sent (Array of Strings or String)
        # @param [Boolean] even_if_busy - ignore busyness of connections
        # @return [Boolean] whether message were sent
        def send_message(message, even_if_busy = false)
          if connect = choose_peer(even_if_busy)
            connect.send_strings(message)
            true
          end
        end
      end

      # ZMQ socket which tries to be a lot like a Dealer. It stores messages into outgoing
      # queue, it tries to balance on socket busyness (which is slightely more, than ZMQ do).
      # The only visible change from PreDealer is less frequent +send_message+ false return.
      #
      # @example
      # class MyDealer < EM::Protocols::Zmq2::Dealer
      #   def receive_message(message)
      #     puts "Message received: #{message.inspect}"
      #   end
      # end
      # dealer = MyDealer.new
      # dealer.connect('tcp://127.0.0.1:8000')
      # if !dealer.send_message(['asdf','fdas'])
      #   puts "No free peers and outgoing queue is full"
      # end
      class Dealer < PreDealer
        # :stopdoc:
        def initialize(opts = {})
          super
          @write_queue = []
        end

        alias raw_send_message send_message
        def flush_queue(even_if_busy = false)
          until @write_queue.empty?
            return false  unless raw_send_message(@write_queue.first, even_if_busy)
            @write_queue.shift
          end
          true
        end

        def send_message(message)
          flush_queue && super(message) || push_to_queue(@write_queue, message)
        end

        def peer_free(peer_identity, connection)
          super
          flush_queue
        end

      private
        def flush_all_queue
          flush_queue(true)
        end

        def react_on_hwm_decrease
          push_to_queue(@write_queue)
        end
        # :startdoc:
      end

      # Convinient Dealer class which accepts callback in constructor, which will be called
      # on every incoming message (instead of #receive_message)
      #
      # @example
      # dealer = EM::Protocols::Zmq2::DealerCb.new do |message|
      #   puts "Receive message #{message.inspect}"
      # end
      # dealer.connect('ipc://rep')
      # dealer.send_message(['hello','world'])
      class DealerCb < Dealer
        # Accepts callback as second parameter or block
        # :callsec:
        #   new(opts) do |message|   end
        #   new(opts, proc{|message| })
        def initialize(opts = {}, cb = nil, &block)
          super opts
          @read_callback = cb || block
        end

        def receive_message(message) # :nodoc:
          @read_callback.call message
        end
      end
    end
  end
end
