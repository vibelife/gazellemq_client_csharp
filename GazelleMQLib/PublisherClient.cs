using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;

namespace GazelleMQLib;

public class PublisherClient {
    private readonly Socket _socket = new(SocketType.Stream, ProtocolType.Tcp);
    private readonly EventWaitHandle _waitHandle = new(false, EventResetMode.ManualReset);
    private ConcurrentQueue<string> _queue = new();
    private bool _isRunning = true;
    private string _nextBatch = "";
    private string _writeBuffer = "";
    private uint _messageBatchSize = 10;

    private PublisherClient() {
        
    }

    public static PublisherClient Create() {
        return new PublisherClient();
    }

    /**
     * Sets the message batch size. Play with this value until you get the desired
     * performance numbers. The default is 10.
     */
    public PublisherClient SetMessageBatchSize(uint value) {
        this._messageBatchSize = value;
        return this;
    }
    
    /**
     * Connects to the GazelleMQ server
     * @param name - The name of this subscriber
     * @param onReady - The function to call after connecting to the hub
     * @param host - The host of the GazelleMQ server
     * @param port - The port of the GazelleMQ server
     */
    public void ConnectToHub(string name, Action? onReady = null, string host = "localhost", int port = 5875) {
        // run the queue processor in a background thread
        new Thread(RunQueueProcessor).Start();

        _socket.Connect(host, port);

        // send the intent
        _socket.Send("P\r"u8);
        // send the client name
        _socket.Send(Encoding.UTF8.GetBytes(name + "\r"));
        // receive a 1 byte ack
        _socket.Receive(new byte[1]);
        // at this point we have registered on the hub, and now we can publish
        onReady?.Invoke();
    }

    /**
     * Publishes a message to the hub for subscribers to consume. It is safe to call
     * this method before the client has connected to the hub.
     * @param type    - The type of message. Subscribers need to subscriber to this
     *                  in order to receive the messages.
     * @param content - The message to publish to subscribers.
     */
    public void Publish(string type, string content) {
        _queue.Enqueue(String.Concat(type, "|", content.Length, "|", content));
        _waitHandle.Set();
    }

    /**
     * Drains the queue and publishes each message in it to the hub
     */
    private async Task<bool> DrainQueue() {
        string tmp = "";
        _nextBatch = "";

        int i = 0;
        while ((i != _messageBatchSize) && _queue.TryDequeue(out tmp)) {
            _nextBatch = string.Concat(_nextBatch, tmp);
            ++i;
        }

        _writeBuffer = _nextBatch;
        if (_writeBuffer.Length > 0) {
            int bytesSent = 0;
            int messageLength = _writeBuffer.Length;
            while (bytesSent < messageLength) {
                bytesSent += await _socket.SendAsync(Encoding.UTF8.GetBytes(_writeBuffer));
            }

            return true;
        }

        return false;
    }

    /**
     * Processes the queue draining it every time it gets populated.
     * Each item pulled off the queue is sent asynchronously to the hub.
     */
    private async void RunQueueProcessor() {
        while (_isRunning) {
            if (!_queue.IsEmpty || _waitHandle.WaitOne()) {
                try {
                    await DrainQueue();
                }
                finally {
                    _waitHandle.Reset();
                }
            }
        }
    }
}