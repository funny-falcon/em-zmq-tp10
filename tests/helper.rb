require 'ffi-rzmq'
module Native
  extend self
  ZBIND_ADDR = 'tcp://127.0.0.1:7890'
  ZCONNECT_ADDR = 'tcp://127.0.0.1:7891'

  def native_context
    @zctx = ZMQ::Context.new
  end

  def with_context
    native_context
    yield
  ensure
    close_context
  end

  def close_context
    @zctx.terminate
  end

  def setup_pair(kind)
    kind_n = ZMQ.const_get(kind)
    @zbind = @zctx.socket(kind_n)
    @zbind.identity = "BIND_#{kind}"
    @zbind.bind(ZBIND_ADDR)
    @zconnect = @zctx.socket(kind_n)
    @zconnect.identity = "CONNECT_#{kind}"
    @zconnect.connect(ZCONNECT_ADDR)
    [@zbind, @zconnect]
  end

  def with_socket_pair(kind)
    with_context do
      begin
        yield setup_pair(kind)
      ensure
        close_pair
      end
    end
  end

  def close_pair
    @zbind.setsockopt(ZMQ::LINGER, 0)
    @zconnect.setsockopt(ZMQ::LINGER, 0)
    @zbind.close
    @zconnect.close
  end

  def setup_socket(kind)
    kind_n = ZMQ.const_get(kind)
    @zbind = @zctx.socket(kind_n)
    @zbind.identity = "BIND_#{kind}"
    @zbind.bind(ZBIND_ADDR)
    @zbind
  end

  def with_socket(kind)
    with_context do
      begin
        yield setup_socket(kind)
      ensure
        close_socket
      end
    end
  end

  def close_socket
    @zbind.setsockopt(ZMQ::LINGER, 0)
    @zbind.close
  end
end
