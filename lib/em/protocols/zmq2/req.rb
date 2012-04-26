require 'em/protocols/zmq2/dealer'
module EventMachine
  module Protocols
    module Zmq2
      # ZMQ socket which acts like REQ, but without outgoing message queueing,
      # It generates unique request ids and uses ZMQ routing scheme for mapping
      # replies.
      #
      # Note, that on subclassing, you should override +#receive_reply+ and not
      # +#receive_message+ , and you should use +#send_request+ instead of
      # +#send_message+
      #
      # @example
      #   class MyReq < EM::Protocols::Zmq2::PreReq
      #     def receive_reply(message, data, request_id)
      #       puts "received message #{message} and stored data #{data}
      #     end
      #   end
      #
      #   req = MyReq.new
      #   if request_id = req.send_request(['hi'], 'ho')
      #     puts "Message sent"
      #   end
      #
      # @example
      #   class TimeoutedPreReq < EM::Protocols::Zmq2::PreReq
      #     def initialize(opts={})
      #       super
      #       @timeout = opts[:timeout] || 1
      #     end
      #     def cancel_request(request_id)
      #       data = super
      #       EM.cancel_timer(data[:timer])  if data[:timer]
      #       data[:data]
      #     end
      #     def receive_reply(message, data, request_id)
      #       if timer = data[:timer]
      #         EM.cancel_timer(data[:timer])
      #       end
      #       puts "receive message #{message.inspect}, associated data #{data[:data].inspect}"
      #     end
      #     def send_request(message, data)
      #       data = {data: data}
      #       if request_id = super(message, data)
      #         data[:timer] = EM.add_timer(@timeout){ cancel_request(request_id) }
      #       end
      #       request_id
      #     end
      #   end
      #   req = TimeoutedPreReq.new
      #   req.bind('ipc://req')
      #   callback_data = { some_data: "" }
      #   if request_id = req.send_request(['hello', 'world'], callback_data)
      #     puts "Request sent with request_id #{request_id}"
      #   else
      #     puts "No free peers"
      #   end
      class PreReq < PreDealer
        def initialize(opts = {}) # :nodoc:
          super
          @data = {}
        end

        # cancel pending request, so that callback will not be called on incoming
        # message.
        # @return associated data with request_id
        def cancel_request(request_id)
          @data.delete request_id
        end

        # do not override +#receive_message+ or for PreReq subclasses
        def receive_message(message)
          request_id, message = split_message(message)
          request_id = request_id.first
          if data = @data.delete(request_id)
            receive_reply(message, data, request_id)
          end
        end

        # override it to react on incoming message
        # Also accept saved data by +#send_request+
        def receive_reply(message, data, request_id)
          raise NoMethodError
        end

        def form_request(message) # :nodoc:
          request_id = next_uniq_identity # I believe, it large enough
          [request_id, EMPTY, *message]
        end

        # send request and store associated callback data
        # Returns assigned request id or nil, if sending is unsuccessfull
        def send_request(message, data, even_if_busy = false)
          request = form_request(message)
          request_id = request.first
          @data[request_id] = data
          if send_message(request, even_if_busy)
            request_id
          else
            @data.delete request_id
            false
          end
        end

      private :send_message
      private
        def cancel_message(message) # :nodoc:
          cancel_request(message.first)
        end
      end

      # ZMQ socket which acts like REQ. It also reacts on connection busyness, so
      # that it is a bit smarter, than ZMQ REQ.
      # It generates unique request ids and uses ZMQ routing scheme for mapping
      # replies.
      #
      # The only visible change from PreReq is less frequent +send_request+ false return
      #
      # Note, that on subclassing, you should override +#receive_reply+ and not
      # +#receive_message+ , and you should use +#send_request+ instead of
      # +#send_message+
      #
      # @example
      #   class MyReq < EM::Protocols::Zmq2::Req
      #     def receive_reply(message, data, request_id)
      #       puts "received message #{message} and stored data #{data}
      #     end
      #   end
      #
      #   req = MyReq.new
      #   if request_id = req.send_request('hi', 'ho')
      #     puts "Message sent"
      #   end
      #
      # @example
      #   class TimeoutedReq < EM::Protocols::Zmq2::PreReq
      #     def initialize(opts={})
      #       super
      #       @timeout = opts[:timeout] || 1
      #     end
      #     def cancel_request(request_id)
      #       data = super
      #       EM.cancel_timer(data[:timer])  if data[:timer]
      #       data[:data]
      #     end
      #     def receive_reply(message, data)
      #       if timer = data[:timer]
      #         EM.cancel_timer(data[:timer])
      #       end
      #       puts "receive message #{message.inspect}, associated data #{data[:data].inspect}"
      #     end
      #     def send_request(message, data)
      #       data = {data: data}
      #       if request_id = super(message, data)
      #         data[:timer] = EM.add_timer(@timeout){ cancel_request(request_id) }
      #       end
      #       request_id
      #     end
      #   end
      #   req = TimeoutedReq.new
      #   req.bind('ipc://req')
      #   callback_data = { some_data: "" }
      #   if request_id = req.send_request(['hello', 'world'], callback_data)
      #     puts "Request sent with request_id #{request_id}"
      #   else
      #     puts "No free peers and highwatermark reached"
      #   end
      class Req < PreReq
        def initialize(opts = {})
          super
          @requests = {}
        end

        # cancel pending request, so that callback will not be called on incoming
        # message
        # @return associated data with request_id
        def cancel_request(request_id)
          @requests.delete request_id
          @data.delete request_id
        end

        def send_request(message, data)
          request = form_request(message)
          request_id = request.first
          @data[request_id] = data
          if flush_queue && send_message(request) || push_to_queue(request)
            request_id
          end
        end

        def flush_queue(even_if_busy = false)
          until @requests.empty?
            request_id, request = @requests.first
            return false  unless send_message(request, even_if_busy)
            @requests.delete(request_id)
          end
          true
        end

        def flush_all_queue # :nodoc:
          flush_queue(true)
        end

        private

        def peer_free(peer, connection)
          super
          flush_queue
        end

        def react_on_hwm_decrease
          push_to_queue
        end

        def cancel_message(message)
          cancel_request(message.first)
        end

        def push_to_queue(request = nil)
          if @requests.size >= @hwm
            case @hwm_strategy
            when :drop_last
              if @requests.size > @hwm
                new_requests = {}
                @requests.each do |k,message|
                  if new_requests.size >= @hwm
                    cancel_message(message)
                  else
                    new_requests[k] = message
                  end
                end
                @requests = new_requests
              end
              false
            when :drop_first
              hwm = @hwm - (request ? 1 : 0)
              while @requests.size > hwm
                k, message = @requests.shift
                cancel_message(message)
              end
              @requests[request.first] = request.dup  if request
              true
            end
          else
            @requests[request.first] = request.dup  if request
            true
          end
        end
      end

      # Convinient Req class which accepts callback as data
      #
      # @example
      #   req = EM::Protocols::Zmq2::ReqCb.new
      #   req.bind('ipc://req')
      #   timer = nil
      #   request_id = req.send_request(['hello', 'world']) do |message|
      #     EM.cancel_timer(timer)
      #     puts "Message #{message}"
      #   end
      #   if request_id
      #     timer = EM.add_timer(1) {
      #       req.cancel_request(request_id)
      #     }
      #   end
      class ReqCb < Req
        def receive_reply(message, callback, request_id)
          callback.call(message, request_id)
        end

        def send_request(message, callback = nil, &block)
          super message, callback || block
        end
      end

      # Convinient Req class, which returns EM::DefaultDeferable on +#send_request+
      #
      # @example
      #   req = EM::Protocolse::Zmq2::ReqDefer.new
      #   req.bind('ipc://req')
      #   data = {hi: 'ho'}
      #   deferable = req.send_request(['hello', 'world'], data) do |reply, data|
      #     puts "Reply received #{reply} #{data}"
      #   end
      #   deferable.timeout 1
      #   deferable.errback do
      #     puts "Message canceled"
      #   end
      #   deferable.callback do |reply, data|
      #     puts "Another callback #{reply} #{data}"
      #   end
      class ReqDefer < Req
        class Wrapped < ::Struct.new(:data, :deferrable); end

        def cancel_request(request_id)
          wrapped = super
          wrapped.deferrable.fail(nil, wrapped.data)
        end
        def receive_reply(reply, wrapped, request_id)
          wrapped.deferrable.succeed(reply, wrapped.data)
        end
        def send_request(message, data, callback = nil, &block)
          deferrable = EM::DefaultDeferrable.new
          wrapped = Wrapped.new(data, deferrable)
          callback ||= block
          if Proc === callback
            deferrable.callback &callback
          else
            deferrable.callback{|reply, data| callback.call(reply, data)}
          end
          if request_id = super(message, wrapped)
            deferrable.errback{ cancel_request(request_id) }
          else
            deferrable.fail(nil, data)
          end
          deferrable
        end
      end

    end
  end
end
