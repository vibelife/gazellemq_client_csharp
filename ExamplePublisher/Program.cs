// See https://aka.ms/new-console-template for more information
using GazelleMQLib;

var client = PublisherClient.Create()
    .SetMessageBatchSize(20);

client.ConnectToHub("CSharpClient", () => {
    Console.WriteLine(DateTimeOffset.Now.ToUnixTimeMilliseconds());
});

for (var i = 0; i < 1000000; ++i) {
    client.Publish("order", "{\"peer\":\"EURUSD\", \"type\": \"buy\"}");
}
