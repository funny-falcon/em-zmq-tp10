require 'eventmachine'
require 'minitest/autorun'
require File.expand_path('../helper.rb', __FILE__)

require 'em/protocols/zmq2/socket'

describe EM::Protocols::Zmq2::Socket do
  class MSocket < EM::Protocols::Zmq2::Socket
    include DeferredMixin
  end
  let(:connected){ EM::DefaultDeferrable.new }
  let(:fan) { MSocket.new(connected: connected) }
  let(:sink) { MSocket.new() }

  it 'should connect after timeout to tcp' do
    were_connected = false
    EM.run {
      connected.callback {
        were_connected = true
        EM.next_tick { EM.stop }
      }
      fan.connect(Native::ZBIND_ADDR)
      fan.connect(Native::ZCONNECT_ADDR)
      EM.add_timer(0.1) do
        sink.bind(Native::ZBIND_ADDR)
        sink.bind(Native::ZCONNECT_ADDR)
        EM.add_timer(0.1) do
          EM.next_tick { EM.stop }
        end
      end
    }
    were_connected.must_equal true
  end

  unless RUBY_PLATFORM =~ /windows|mingw|mswin/
    it 'should connect after timeout to ipc' do
      socket1 = File.expand_path("../test1.sock", __FILE__)
      socket2 = File.expand_path("../test2.sock", __FILE__)
      File.unlink(socket1) rescue nil
      File.unlink(socket2) rescue nil

      were_connected = false
      EM.run {
        connected.callback {
          were_connected = true
          EM.next_tick { EM.stop }
        }
        fan.connect('ipc://'+socket1)
        fan.connect('ipc://'+socket2)
        EM.add_timer(0.1) do
          sink.bind('ipc://'+socket1)
          sink.bind('ipc://'+socket2)
          EM.add_timer(0.1) do
            EM.next_tick { EM.stop }
          end
        end
      }
      were_connected.must_equal true

      File.unlink(socket1) rescue nil
      File.unlink(socket2) rescue nil
    end
  end
end
