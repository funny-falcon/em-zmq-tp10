require 'em/protocols/zmq2/connection'
module EventMachine
  module Protocols
    module Zmq2
      module InProc
        class AlreadyBinded < RuntimeError; end
        class NotBinded < RuntimeError; end
        BindPoints = {}
        def self.bind(name, socket)
          unless BindPoints[name].nil? || BindPoints[name] == socket
            raise AlreadyBinded, "ZMQ inproc://#{name} already binded"
          end
          BindPoints[name] = socket
          unless BindPoints[:shutdown_hook_set]
            BindPoints[:shutdown_hook_set] = true
            EM.add_shutdown_hook { BindPoints.clear }
          end
          name
        end

        def self.unbind(name, socket)
          unless BindPoints[name] == socket
            unless BindPoints[name]
              raise NotBinded, "ZMQ bind point inproc://#{name} binded to other socket"
            else
              raise NotBinded, "ZMQ bind point inproc://#{name} is not binded"
            end
          end
          BindPoints.delete name
        end

        def self.connect(name, socket)
          connect = Connection.new(socket)
          EM.next_tick {
            if server = BindPoints[name]
              sconnect = Connection.new(server)
              sconnect.peer = connect
              connect.peer = sconnect
            else
              connect.unbind
            end
          }
          connect
        end

        class Connection
          include ConnectionMixin
          attr :peer
          attr_accessor :peer_waiting, :notify_when_free
          def initialize(socket)
            @socket = socket
            @peer = nil
            @peer_waiting = false
            @outgoing_queue = []
            @recursion = 0
            @state = :connecting
          end

          def peer=(peer)
            raise "Already has peer"  if @peer
            @state = :connected
            @peer = peer
            peer.peer_waiting!
            EM.next_tick { post_init }
          end

          def _not_too_busy?
            @state == :connected && @outgoing_queue.size <= 32
          end

          def shift_outgoing_queue
            message = @outgoing_queue.shift
            EM.next_tick { sent_data }  if @notify_when_free && _not_too_busy?
            message
          end

          def call
            if message = @peer.shift_outgoing_queue
              receive_strings(message)
            else
              @peer.peer_waiting!
            end
          end

          def receive_strings(message)
            super
            @peer.peer_waiting!
          end

          def peer_waiting!
            unless @outgoing_queue.empty?
              @peer_waiting = false
              if (@recursion += 1) == 10
                EM.next_tick @peer
              else
                @peer.call
              end
            else
              unbind  if @state == :closing
              @peer_waiting = true
            end
          end

          def send_strings(strings)
            if @state == :connected
              unless Array === strings
                strings = Array(strings)
              end
              @outgoing_queue << strings
            end
            if @peer_waiting
              @peer_waiting = false
              EM.next_tick @peer
            end
          end

          def send_strings_or_prepared(strings, prepared)
            send_strings(strings)
          end

          def close_connection(after_writting = false)
            @peer.close_connection(:peer)  unless after_writting == :peer
            unless after_writting == true
              @outgoing_queue.clear
            end
            if @peer_waiting
              EM.next_tick @peer
            end
            @state = :closing
          end

          def unbind
            old, @state = @state, :closed
            if old == :closing
              @peer.peer_waiting!
            end
            super
          end

          def error?
            false
          end
        end
      end
    end
  end
end
