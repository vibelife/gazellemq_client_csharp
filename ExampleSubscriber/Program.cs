// See https://aka.ms/new-console-template for more information

using GazelleMQLib;

SubscriberClient.Create()
    .SetNbHandlerThreads(1)
    .Subscribe("order", msg => {
        Console.WriteLine(msg);
    })
    .ConnectToHub("TestSub");