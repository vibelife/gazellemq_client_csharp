using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;

namespace GazelleMQLib;

public class SubscriberClient {
    private enum ParseState {
        MessageType,
        MessageContentLength,
        MessageContent,
    };

    public delegate void Subscription(string message);

    public struct MessageHandler {
        public string _type;
        public Subscription _subscription;
    }

    private struct Message {
        public string _type;
        public string _content;
    }

    private readonly Socket _socket = new(SocketType.Stream, ProtocolType.Tcp);
    private string _subscriptionNames = "";
    private IList<MessageHandler> _handlers = new List<MessageHandler>();
    private byte[] _readBuffer = new byte[128];
    private ConcurrentQueue<Message> _queue = new ();

    private int _messageContentLength = 0;
    private int _nbContentBytesRead = 0;
    private string _messageContent = "";
    private string _messageLengthBuffer = "";
    private string _messageType = "";
    private ParseState _parseState = ParseState.MessageType;
    private readonly EventWaitHandle _waitHandle = new(false, EventResetMode.ManualReset);
    private int _nbHandlerThreads = 1;



    private SubscriberClient() { }

    public static SubscriberClient Create() {
        return new SubscriberClient();
    }

    public SubscriberClient SetNbHandlerThreads(int value) {
        this._nbHandlerThreads = value;
        return this;
    }

    /**
     * Subscribes to message of the passed in type
     */
    public SubscriberClient Subscribe(string type, Subscription subscription) {
        _subscriptionNames = string.Concat(_subscriptionNames, ",", type);
        var handler = new MessageHandler();
        handler._subscription = subscription;
        handler._type = type;
        _handlers.Add(handler);
        return this;
    }

    /**
     * Connects to the GazelleMQ server
     */
    public void ConnectToHub(string name, string host = "localhost", int port = 5875) {
        // run the queue processor in a background thread
        MonitorMessageQueue();
        
        _socket.Connect(host, port);

        // send the intent
        _socket.Send("S\r"u8);
        // send the client name
        _socket.Send(Encoding.UTF8.GetBytes(name + "\r"));
        // receive a 1 byte ack
        _socket.Receive(new byte[1]);
        // send the subscriptions
        _subscriptionNames = String.Concat(_subscriptionNames, "\r");
        _socket.Send(Encoding.UTF8.GetBytes(_subscriptionNames));

        // at this point we have registered on the hub, and now we can listen
        // for messages to come in from the hub
        Console.WriteLine("Waiting for messages...");

        string messageData = "";
        while (_socket.Receive(_readBuffer, _readBuffer.Length, 0) > 0) {
            messageData = string.Concat(messageData, Encoding.UTF8.GetString(_readBuffer));
            ParseMessage(messageData);
        }
    }

    /**
     * Parses the message out of the buffer of characters
     */
    private void ParseMessage(string buffer) {
        int bufferLength = buffer.Length;
        for (var i = 0; i < bufferLength; ++i) {
            char ch = buffer[i];

            if (_parseState == ParseState.MessageType) {
                if (ch == '|') {
                    _parseState = ParseState.MessageContentLength;
                    continue;
                }
                else {
                    _messageType = string.Concat(_messageType, ch);
                }
            }
            else if (_parseState == ParseState.MessageContentLength) {
                if (ch == '|') {
                    _messageContentLength = int.Parse(_messageLengthBuffer);
                    _parseState = ParseState.MessageContent;
                    continue;
                }
                else {
                    _messageLengthBuffer = string.Concat(_messageLengthBuffer, ch);
                }
            }
            else if (_parseState == ParseState.MessageContent) {
                int nbCharsNeeded = _messageContentLength - _nbContentBytesRead;
                // add as many characters as possible in bulk

                if ((i + nbCharsNeeded) <= buffer.Length) {
                    _messageContent = string.Concat(_messageContent, buffer.Substring(i, nbCharsNeeded));
                }
                else {
                    nbCharsNeeded = bufferLength - i;
                    _messageContent = string.Concat(_messageContent, buffer.Substring(i, nbCharsNeeded));
                }

                i += nbCharsNeeded - 1;
                _nbContentBytesRead += nbCharsNeeded;

                if (_messageContentLength == _nbContentBytesRead) {
                    // Done parsing
                    var msg = new Message();
                    msg._type = _messageType;
                    msg._content = _messageContent;
                    _queue.Enqueue(msg);
                    _waitHandle.Set();

                    _messageContentLength = 0;
                    _nbContentBytesRead = 0;
                    _messageContent = "";
                    _messageLengthBuffer = "";
                    _messageType = "";
                    _parseState = ParseState.MessageType;
                }
            }
        }
    }

    /**
     * Monitors the queue for new messages. Invokes the appropriate handler with the message. 
     */
    private void MonitorMessageQueue() {
        for (var i = 0; i < _nbHandlerThreads; ++i) {
            var thread = new Thread(o => {
                while (!_queue.IsEmpty || _waitHandle.WaitOne()) {
                    Message m;
                    try {
                        if (_queue.TryDequeue(out m)) {
                            foreach (var handler in _handlers) {
                                if (handler._type.Equals(m._type)) {
                                    handler._subscription.Invoke(m._content);
                                }
                            }
                        }
                    }
                    finally {
                        _waitHandle.Reset();
                    }
                }
            });
            
            thread.Start();
        }
    }
}