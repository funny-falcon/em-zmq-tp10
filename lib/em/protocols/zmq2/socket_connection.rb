require 'eventmachine'
require 'em/protocols/zmq2'
require 'em/protocols/zmq2/connection'
module EventMachine
  module Protocols
    module Zmq2
      # Main implementation peace, heart of protocol
      module PackString
        BIG_PACK = 'CNNCa*'.freeze
        SMALL_PACK = 'CCa*'.freeze
        def pack_string(string, more = false)
          bytesize = string.bytesize + 1
          if bytesize <= 254
            [bytesize, more ? 1 : 0, string].pack(SMALL_PACK)
          else
            [255, 0, bytesize, more ? 1 : 0, string].pack(BIG_PACK)
          end
        end

        def prepare_message(message)
          buffer = ''
          i = 0
          last = message.size - 1
          while i < last
            buffer << pack_string(message[i], true)
            i += 1
          end
          buffer << pack_string(message[last], false)
        end
      end

      # Heavy duty worker class
      # Implements ZMTP1.0 - ZMQ2.x transport protocol
      #
      # It calls following callbacks
      #
      # It is not for end user usage
      class SocketConnection < EM::Connection
        include ConnectionMixin
        # :stopdoc:
        def initialize(socket)
          @socket = socket
          @recv_buffer = ''
          @recv_frames = [[]]
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
          while message = pop_message
            receive_strings(message)
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

        include PackString

        def send_strings(strings)
          if String === strings
            send_data pack_string(strings, false)
          else
            strings = Array(strings)
            send_data prepare_message(strings)
          end
        end
      end
    end
  end
end
