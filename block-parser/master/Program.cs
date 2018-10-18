using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using RabbitMQ.Client;

namespace master
{
    class Program
    {
        public static void Main(string[] args)
        {
            var server = "bitcoin-parser-work-queue-service";
            //var server = "localhost";
            Console.WriteLine(" Welcome! we will connect to rabbit mq now.");
            Console.WriteLine("We will connect to {0} ",server);
            var factory = new ConnectionFactory() { HostName = server };
            using(var connection = factory.CreateConnection())
            using(var channel = connection.CreateModel())
            {
                Console.WriteLine(" Connection to rabbit mq succedded.");

                channel.QueueDeclare(queue: "task_queue",
                                    durable: true,
                                    exclusive: false,
                                    autoDelete: false,
                                    arguments: null);

                var fileLinks = GetBitcoinFilesFromGoogle();

                foreach (var fileLink in fileLinks)
                {
                    var body = Encoding.UTF8.GetBytes(fileLink);

                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;

                    channel.BasicPublish(exchange: "",
                                        routingKey: "task_queue",
                                        basicProperties: properties,
                                        body: body);
                }

                Console.WriteLine(" No of files sent for processing {0}", fileLinks.Count());
            }

            Console.WriteLine(" End.");
            Environment.Exit(0);
        }

        private static IEnumerable<string> GetBitcoinFilesFromGoogle()
        {
            // If you don't specify credentials when constructing the client, the
            // client library will look for credentials in the environment.
            var credential = GoogleCredential.GetApplicationDefault();
            var storage = StorageClient.Create(credential);
            // Make an authenticated API request.
            var inputFiles = storage.ListObjects("bitcoin-flat-bucket","output/")
                    .Where(l => l.Name.EndsWith(".gz"));

            foreach(var file in inputFiles)
            {
                yield return file.Name;
            }
        }
    }
}
