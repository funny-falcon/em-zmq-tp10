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
          if String === message
            pack_string(message, false)
          else
            message = Array(message)
            buffer = ''
            i = 0
            last = message.size - 1
            while i < last
              buffer << pack_string(message[i].to_s, true)
              i += 1
            end
            buffer << pack_string(message[last].to_s, false)
          end
        end
      end

      # Heavy duty worker class
      # Implements ZMTP1.0 - ZMQ2.x transport protocol
      #
      # It calls following callbacks
      #
      # It is not for end user usage
      class SocketConnection < EM::Connection
        # :stopdoc:
        include ConnectionMixin

        def initialize(socket)
          @socket = socket
          @recv_buffer = ''
          @recv_frames = [[]]
        end

        if method_defined?(:outbound_data_count)
          def _not_too_busy?
            outbound_data_count < 32
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

        FF = "\xff".force_encoding('BINARY').freeze
        BIG_UNPACK = 'CNNC'.freeze
        SMALL_UNPACK = 'CC'.freeze
        def parse_frames(data)
          unless @recv_buffer.empty?
            data = @recv_buffer << data
          end
          while data.bytesize >= 2
            if data.start_with?(FF)
              break  if data.bytesize < 10
              _, _, length, more = data.unpack(BIG_UNPACK)
              start_at = 10
            else
              length = data.getbyte(0)
              more = data.getbyte(1)
              start_at = 2
            end
            length -= 1
            finish = start_at + length
            break  if data.bytesize < finish
            str = data.byteslice(start_at, length)
            data = data.byteslice(finish, data.bytesize - finish)
            @recv_frames.last << str
            @recv_frames << [] if more & 1 == 0
          end
          @recv_buffer = data
        end

        include PackString

        def send_strings(strings)
          send_data prepare_message(strings)
        end

        def send_strings_or_prepared(strings, prepared)
          send_data prepared
        end
      end
    end
  end
end
