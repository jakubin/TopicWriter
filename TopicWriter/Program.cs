using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace TopicWriter
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("1 - Receive");
            Console.WriteLine("2 - Send");
            var option = Console.ReadLine();

            if (option == "1")
            {
                ReceiveMessages();
            }
            else if (option == "2")
            {
                SendMessage();
            }
        }

        private static void ReceiveMessages()
        {
            Console.Write("Subscription name: ");
            var subscription = Console.ReadLine();
            var subClient = SubscriptionClient.CreateFromConnectionString(
                "Endpoint=sb://jakubin-test-ns.servicebus.windows.net/;SharedAccessKeyName=Consumer;SharedAccessKey=BA3+dTOdIWkkgoLSr+czqsFCorTzgQ0EAd51+ZjyNSg=",
                "topic123",
                subscription, ReceiveMode.ReceiveAndDelete);

            Console.WriteLine("Reading subscription {0} on topic {1}.", subscription, "topic123");
            subClient.OnMessage(msg =>
            {
                Console.WriteLine("{0} - {1}", msg.Properties["priority"], msg.GetBody<string>());
            });
            Console.WriteLine("Press ENTER to stop listening");
            Console.ReadLine();
            subClient.Close();
        }

        private static void SendMessage()
        {
            try
            {
                while (true)
                {
                    Console.Write("Priority: ");
                    int priority = Int32.Parse(Console.ReadLine());
                    Console.Write("Text: ");
                    string text = Console.ReadLine();
                    var topicClient = TopicClient.CreateFromConnectionString(
                        "Endpoint=sb://jakubin-test-ns.servicebus.windows.net/;SharedAccessKeyName=Publisher;SharedAccessKey=Dk/lqjV6vsfEA9iPp/R7EuvSGYSj5U3bU8PKXsbOBYQ=",
                        "topic123");

                    for (int i = 0; i < 10; i++)
                    {
                        var batch = new List<BrokeredMessage>();
                        for (int j = 0; j < 50; j++)
                        {
                            var message = new BrokeredMessage(text + i)
                            {
                                Properties = { { "priority", priority } }
                            };
                            batch.Add(message);
                        }
                        
                        topicClient.SendBatch(batch);
                    }
                    
                }
            }
            catch (Exception)
            {
            }
        }

        private static void SetupSubscriptions()
        {
            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(
                    "Endpoint=sb://jakubin-test-ns.servicebus.windows.net/;SharedAccessKeyName=Manager;SharedAccessKey=XuKEgyPGl9sD06YN44lXwTjQMCz47sK2mv+SWF2leCY=");

            Console.WriteLine("Creating subscription {0} on topic {1}.", "high-priority", "topic123");
            namespaceManager.CreateSubscription("topic123", "high-priority", new SqlFilter("priority > 3"));
            Console.WriteLine("Creating subscription {0} on topic {1}.", "low-priority", "topic123");
            namespaceManager.CreateSubscription("topic123", "low-priority", new SqlFilter("priority <= 3"));
        }
    }
}
