# EventMachine ZMTP1.0 protocol (ZMQ2.x protocol)

It is implementation of ZMTP 1.0 - ZMQ 2.x transport protocol.
There are implementations of ZMQ socket types which try to be
similar to original, but not too hard.

## Installation

Add this line to your application's Gemfile:

    gem 'em-zmq-tp10'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install em-zmq-tp10

## Usage

Library provides callback oriented classes which tries to emulate behaviour of standard ZMQ classes.

Main difference in behaviour is in highwatermark handling and balancing:
For DEALER and REQ zmq provides roundrobin load balancing until HighWaterMark reached, then send operation blocks or returns error `EAGAIN`.
This implementation do roundrobin until fixed internal highwatermark (2048bytes) reached, and then pushes to common queue until userdefined watermark is reached.

This internal per connect buffer is handled by EventMachine itself, and there is no precise control over it, so that, if peer is disconnected, all pushed messages to this buffer are lost. So that, compared to raw ZMQ, you loose not only content of OS's internal socket buffer, but EventMachine buffer as well :(

But since ZMQ is never pretending on durability, it is not big issue (for me).

There is two strategy of highwatermark handling: `drop_first` and `drop_last`.
`drop_last` - is ignoring any try to send message if queue is full - this is default strategy for ZMQ if you use nonblocking sending.
`drop_first` - dropping earliest message in a queue, so that newly inserted message will have more chanches to be sent. You can react on such dropping by overriding `cancel_message` (or `cancel_request` for Req). I like this strategy more cause old request tends to be less usefull, but `drop_last` is still default for "compatibility".

There is also simplified classes without internal queue (`PreDealer`, `PreReq`, `PreRouter`, `PreRep`, `PrePub`), so that you can implement your own strategy of queueing.

And you could do any crazy thing using base `EM::Protocols::Zmq2::Socket` class


### Socket

  Base class. It provides #connect and #bind methods for establishing endpoints.
  This method could be called outside EM event loop (even before EM.run called), cause they use EM.schedule.
  At the moment only TCP and IPC connection types are supported.

### Dealer

      class MyPreDealer < EM::Protocols::Zmq2::PreDealer
        def receive_message(message)
          puts "Message received: #{message.inspect}"
        end
      end
      dealer = MyPreDealer.new
      dealer.connect('tcp://127.0.0.1:8000')
      dealer.bind('unix://dealer.sock')
      EM.schedule {
        if !dealer.send_message(['asdf','fdas'])
          puts "Could not send message (no free peers)"
        end
      }

      class MyDealer < EM::Protocols::Zmq2::Dealer
        def receive_message(message)
          puts "Message received: #{message.inspect}"
        end
      end
      dealer = MyDealer.new(hwm: 1000, hwm_strategy: :drop_last)
      dealer.connect('tcp://127.0.0.1:8000')
      EM.schedule {
        if !dealer.send_message(['asdf','fdas'])
          puts "No free peers and outgoing queue is full"
        end
      }

      dealer = EM::Protocols::Zmq2::DealerCb.new do |message|
         puts "Receive message #{message.inspect}"
      end
      dealer.connect('ipc://rep')
      EM.schedule {
        dealer.send_message(['hello','world'])
      }

### Req

      class MyPreReq < EM::Protocols::Zmq2::PreReq
        def receive_reply(message, data, request_id)
          puts "Received message #{message} and stored data #{data}
        end
      end
      req = MyPreReq.new
      req.bind(...)
      req.connect(...)
      if request_id = req.send_request(['this is', 'message'], 'saved_data')
        puts "Message sent"
      else
        puts "there is no free peer"
      end

      class MyReq < EM::Protocols::Zmq2::PreReq
        def receive_reply(message, data, request_id)
          puts "Received message #{message} and stored data #{data}
        end
      end
      req = MyReq.new
      req.bind(...)
      req.connect(...)
      if request_id = req.send_request(['hi'], 'ho')
        puts "Message sent"
      end

      req = EM::Protocols::Zmq2::ReqCb.new
      req.bind('ipc://req')
      timer = nil
      request_id = req.send_request(['hello', 'world']) do |message|
        EM.cancel_timer(timer)
        puts "Message #{message}"
      end
      if request_id
        timer = EM.add_timer(1) {
          req.cancel_request(request_id)
        }
      end

      req = EM::Protocols::Zmq2::ReqDefer.new
      req.bind('ipc://req')
      data = {hi: 'ho'}
      deferable = req.send_request(['hello', 'world'], data) do |reply, data|
        puts "Reply received #{reply} #{data}"
      end
      deferable.timeout 1
      deferable.errback do
        puts "Message canceled"
      end
      deferable.callback do |reply, data|
        puts "Another callback #{reply} #{data}"
      end

### Router

Router stores peer identity in a message, as ZMQ router do.
And it sends message to a peer, which idenitity equals to first message string.
`PreRouter` does no any queue caching, `Router` saves message in queue per peer, controlled by highwatermark strategy.

      class MyPreRouter < EM::Protocols::Zmq2::PreRouter
        def receive_message(message)
          puts "Received message #{message} (and it contains envelope)"
        end
      end
      router = MyPreRouter.new
      router.bind(...)
      router.send_message(message)

      class MyRouter < EM::Protocols::Zmq2::Router
        def receive_message(message)
          puts "Received message #{message}"
          message[-1] = 'reply'
          send_message(message)
        end
      end
      router = MyPreRouter.new(hwm: 1000, hwm_strategy: :drop_first)
      router.bind(...)
      router.send_message(message)

### Rep

REP differs from Router mainly in methods signature.

      class EchoBangPreRep < EM::Protocols::Zmq2::PreRep
        def receive_request(message, envelope)
          message << "!"
          if send_reply(message, envelope)
            puts "reply sent successfuly"
          end
        end
      end
      rep = EchoBangPreRep.new
      rep.bind('ipc://rep')

      class EchoBangRep < EM::Protocols::Zmq2::Rep
        def receive_request(message, envelope)
          message << "!"
          if send_reply(message, envelope)
            puts "reply sent successfuly"
          end
        end
      end
      rep = EchoBangRep.new
      rep.bind('ipc://rep')

### Sub

Unless ZMQ sub, this `Sub` accepts not only strings, but also RegExps and procs for subscribing.
Note that as in ZMQ 2.x filtering occurs on Sub side.

Since subscriptions could be defined with callback passed to `:subscribe` option, `subscribe` or `subscribe_many` methods, you could use this class without overloading.

      class MySub < EM::Protocols::Zmq2::Sub
        def receive_message(message)
          puts "default handler"
        end
      end
      sub = MySub.new(subscribe: ['this', 'that'])
      sub.subscribe /^callback/i, do |message|
        puts "Callback subscribe #{message}"
      end
      sub.subscribe_many(
        proc{|s| s.end_with?("END")} => proc{|message| puts "TILL END #{message}"},
        '' => nil # also to default
      )

### Pub

`PrePub` sends messages only to connected and not busy peers. `send_message` returns true, if there is at least one peer with short EventMachine's outgoing queue, to which message is scheduled.

`Pub` tries to queue messages for all connected and for disconnected peers with explicit identity set.

Since there is no incoming data, there is no need to overload methods.

      pub = EM::Protocols::Zmq2::PrePub.new
      pub.bind(...)
      pub.send_message(['hi', 'you'])

      pub = EM::Protocols::Zmq2::Pub.new
      pub.bind(...)
      pub.send_message(['hi', 'you'])


## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
