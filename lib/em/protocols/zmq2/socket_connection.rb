require 'eventmachine'
require 'em/protocols/zmq2'
module EventMachine
  module Protocols
    module Zmq2
      # Heavy duty worker class
      # Implements ZMTP1.0 - ZMQ2.x transport protocol
      #
      # It calls following callbacks
      #
      # It is not for end user usage
      class SocketConnection < EM::Connection
        # :stopdoc:
        def initialize(socket)
          @socket = socket
          @recv_buffer = ''
          @recv_frames = [[]]
        end

        def post_init
          send_frame @socket.identity, false
        end

        def unbind(err)
          if @peer_identity
            @socket.unregister_peer(@peer_identity)
          end
          @socket.not_connected(self)
        end

        # use watching on outbound queue when possible
        # rely on https://github.com/eventmachine/eventmachine/pull/317 if were accepted
        # or on https://github.com/funny-falcon/eventmachine/tree/sent_data
        # use timers otherwise
        def sent_data
          @socket.peer_free(@peer_identity, self) if not_too_busy?
        end

        if method_defined?(:outbound_data_count)
          def _not_too_busy?
            outbound_data_count < 16
          end
        else
          def _not_too_busy?
            get_outbound_data_size < 2048
          end
        end

        if method_defined?(:notify_sent_data=)
          alias notify_when_free= notify_sent_data=
        else
          def notify_when_free=(v)
            if v
              @when_free_timer ||= EM.add_timer(SMALL_TIMEOUT) do
                @when_free_timer = nil
                sent_data
              end
            elsif @when_free_timer
              EM.cancel_timer @when_free_timer
              @when_free_timer = nil
            end
          end
        end

        def not_too_busy?
          free = _not_too_busy?
          self.notify_when_free = !free
          free
        end

        def receive_data(data)
          parse_frames(data)
          if message = pop_message
            unless @peer_identity
              peer_identity = message.first
              message = pop_message
              @peer_identity = @socket.register_peer(peer_identity, self)
            end
            while message
              @socket.receive_message_and_peer message, @peer_identity
              message = pop_message
            end
          end
        end

        def pop_message
          if @recv_frames.size > 1
            @recv_frames.shift
          end
        end

        FF = "\xff".freeze
        BIG_UNPACK = 'CNNC'.freeze
        SMALL_UNPACK = 'CC'.freeze
        BIG_PACK = 'CNNCa*'.freeze
        SMALL_PACK = 'CCa*'.freeze
        def parse_frames(data)
          data = @recv_buffer.empty? ? data : (@recv_buffer << data)
          while data.bytesize > 0
            if data.start_with?(FF)
              _, _, length, more = data.unpack(BIG_UNPACK)
              start_at = 10
            else
              length, more = data.unpack(SMALL_UNPACK)
              start_at = 2
            end
            length -= 1
            break  if data.size < start_at + length
            str = data[start_at, length]
            data[0, start_at + length] = EMPTY
            @recv_frames.last << str
            @recv_frames << [] if more & 1 == 0
          end
          @recv_buffer = data
        end

        def send_frame(string, more = false)
          bytesize = string.bytesize + 1
          if bytesize <= 254
            packed = [bytesize, more ? 1 : 0, string].pack(SMALL_PACK)
          else
            packed = [255, 0, bytesize, more ? 1 : 0, string].pack(BIG_PACK)
          end
          if @buffer
            @buffer << packed
            unless more
              send_data @buffer
              @buffer = nil
            end
          elsif !more
            send_data packed
          else
            @buffer = packed
          end
        end

        def send_strings(strings)
          unless Array === strings
            strings = Array(strings)
          end
          strings[0..-2].each{|str| send_frame(str, true)}
          send_frame(strings.last, false)
        end
      end
    end
  end
end
