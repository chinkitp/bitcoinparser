using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using NBitcoin;
using Google.Cloud.Storage.V1;
using Google.Apis.Auth.OAuth2;
using System.Threading.Tasks;
using NBitcoin.BitcoinCore;
using Newtonsoft.Json;
using System.IO.Compression;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Threading;
using RabbitMQ.Client.MessagePatterns;

namespace dotnet_bitcoinparser
{
    class Program
    {
        static void Main(string[] args)
        {
            InitQueueWorker();
        }

        private static void InitQueueWorker()
        {
            var server = "bitcoin-parser-work-queue-service";
            //var server = "localhost";
            var factory = new ConnectionFactory() { HostName = server };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var q = channel.QueueDeclare(queue: "task_queue",
                                    durable: true,
                                    exclusive: false,
                                    autoDelete: false,
                                    arguments: null);

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                var consumer = new EventingBasicConsumer(channel);
                
                consumer.Received += (model, ea) =>
                {
                    Console.WriteLine($"NO OF FILES TO PROCESS {q.MessageCount}");
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    ProcessFile(message);

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };
                channel.BasicConsume(queue: "task_queue",
                                    autoAck: false,
                                    consumer: consumer);  
                Thread.Sleep(Timeout.Infinite);         
            }
        }

        private static void ProcessFile(string fileName)
        {
            Console.WriteLine($"Processing file {fileName}");

            var blocksAsHex = GetBlocksFromGoogleFile(fileName);
            Parallel.ForEach(blocksAsHex,(item)=>{
                try
                {
                    var block = Block.Parse(item,Network.Main);
                    string blockHash = block.GetHash().ToString();
                    
                    var elementGroups = ToEpgmElements(block)
                                    .GroupBy(e => e.Meta.Label)
                                    .Select(g => {
                                        return new {g.Key, Elements = g.Select(e => JsonConvert.SerializeObject(e))};                                      
                                    });
                                                                
                    foreach (var elementGroup in elementGroups)
                    {
                        var label = elementGroup.Key;
                        WriteGoogleFile(label, $"{blockHash}.txt" ,elementGroup.Elements);    
                    }
                }
                catch (System.Exception ex)
                {
                    Console.WriteLine($"In {fileName} unable to process block : {item} due to {ex.Message}");
                }
            });             
        }

        private static IEnumerable<Epm> ToEpgmElements(Block block)
        {
            var emtpyProps = new Dictionary<string, object>();
            var elements = new List<Epm>();

            var blockVertex = new Vertex(block.Header.ToString(),Label.BLOCK,new Dictionary<string, object>{
                {"PreviousBlockHash",block.Header.HashPrevBlock.ToString()},
                {"Nonce",block.Header.Nonce},
                {"Difficulty",block.Header.Bits},
                {"Version",block.Header.Version},
                {"Time",block.Header.BlockTime},
                {"MerkleRoot",block.Header.HashMerkleRoot.ToString()}
            });
            elements.Add(blockVertex);

            //previous block
            var previous_block = new Edge(GetGuid(),Label.PREVIOUS,block.Header.ToString(),block.Header.HashPrevBlock.ToString(),emtpyProps);


            foreach (var t in block.Transactions)
            {
                var txData = new Dictionary<string, object>{
                    {"Version",t.Version},
                    {"HasWitness",t.HasWitness},
                    {"NoOfInputs",t.Inputs?.Count},
                    {"NoOfOutputs",t.Outputs?.Count},
                    {"LockTime",t.LockTime.ToString()},
                    {"IsCoinBase",t.IsCoinBase},
                    {"TotalOut",t.TotalOut.Satoshi},
                };

                if(t.IsCoinBase)
                {
                    txData.Add("ScriptLength",t.Inputs[0].ScriptSig?.Length);
                    txData.Add("Script",t.Inputs[0].ScriptSig?.ToHex());                  
                }

                var tx = new Vertex(t.GetHash().ToString(),Label.TRANSACTION,txData);
                
                //edge
                string minded_in_id = block.Header.ToString() + "-" + block.Transactions.IndexOf(t).ToString();
                var mined_in = new Edge(minded_in_id,Label.MINED_IN,t.GetHash().ToString(),block.Header.ToString(),emtpyProps);

                elements.Add(tx);
                elements.Add(mined_in);

                if(!t.IsCoinBase)
                {
                    foreach (var txi in t.Inputs)
                    {


                        //we need to extract the address where it came from
                        var txo = txi.PrevOut.Hash.ToString() + "-" + txi.PrevOut.N;
                        var spent_id = "SPENT : " + txo;

                        var unlocked = new Edge(spent_id,Label.SPENT,txo,t.GetHash().ToString(),new Dictionary<string, object>{
                            { "SignedBy",txi.ScriptSig?.GetSigner()?.ToString()},
                            {"SignedBy_Address",txi.ScriptSig?.GetSignerAddress(Network.Main)?.ToString()},
                            {"ScriptLength",txi.ScriptSig?.Length},                        
                            {"Script",txi.ScriptSig?.ToHex()},                            
                        });

                            elements.Add(unlocked);
                    }
                }

                foreach (var txo in t.Outputs)
                {
                    string txoId = t.GetHash().ToString() + "-" + t.Outputs.IndexOf(txo).ToString();

                    var txoVertex = new Vertex(
                        txoId,
                        "txout",
                        new Dictionary<string, object>{
                            {"Value",txo.Value.Satoshi},
                            {"ScriptLength",txo.ScriptPubKey?.Length},                        
                            {"Script",txo.ScriptPubKey?.ToString()},                    
                        }
                    );
                    elements.Add(txoVertex);
                    elements.Add(new Edge(GetGuid(),Label.PRODUCED,t.GetHash().ToString(),txoId,emtpyProps));

                    if(txo.ScriptPubKey?.FindTemplate()?.Type == TxOutType.TX_PUBKEY)
                    {
                        foreach (var desPubKey in txo.ScriptPubKey?.GetDestinationPublicKeys())
                        {
                            var addressIdFromPubKey = desPubKey.GetAddress(Network.Main).ToString();
                            var desAddressVertex = new Vertex(addressIdFromPubKey,Label.ADDRESS,new Dictionary<string, object>{
                                {"Public_Key", desPubKey.ToString() }
                            });
                            elements.Add(desAddressVertex);
                            elements.Add(new Edge(GetGuid(),Label.LOCKED_AT,txoId,addressIdFromPubKey,emtpyProps));
                        }
                    
                    }
                    else if(txo.ScriptPubKey?.FindTemplate()?.Type == TxOutType.TX_PUBKEYHASH) 
                    {
                        var addressId = txo.ScriptPubKey?.GetDestinationAddress(Network.Main)?.ToString();
                        var desAddressVertex = new Vertex(addressId,Label.ADDRESS,emtpyProps);
                        elements.Add(desAddressVertex);
                        elements.Add(new Edge(GetGuid(),Label.LOCKED_AT,txoId,addressId,emtpyProps));
                    }
          
                }             
            }

            return elements;
        }

        private static string GetGuid()
        {
             return Convert.ToBase64String(Guid.NewGuid().ToByteArray())
             .Substring(0,22);
        }

        private static void WriteGoogleFile(string folder, string fileName, IEnumerable<string> lines)
        {
            var storage = StorageClient.Create();
            StringBuilder sb = new StringBuilder();
            foreach (var line in lines)
            {
                sb.AppendLine(line);
            }
            var asFile = System.Text.Encoding.Default.GetBytes(sb.ToString());
            using (var memoryStream = new MemoryStream(asFile))
            {
                storage.UploadObject("bitcoin-epgm", $"epgm/{folder}/{fileName}", "application/octet-stream", memoryStream); 
            }
        }

        private static IEnumerable<string> GetBlocksFromGoogleFile(string fileName)
        {
            // If you don't specify credentials when constructing the client, the
            // client library will look for credentials in the environment.
            var credential = GoogleCredential.GetApplicationDefault();
            var storage = StorageClient.Create(credential);
            // Make an authenticated API request.
            var file = storage.GetObject("bitcoin-flat-bucket" ,fileName);

            using (var googleStream = new MemoryStream())
            {
                storage.DownloadObject(file, googleStream);       
                var full = googleStream.ToArray();
                googleStream.Position = 0;
                using (GZipStream decompressionStream = new GZipStream(googleStream, CompressionMode.Decompress))
                {
                    using(var ms = new MemoryStream())
                    {
                        decompressionStream.CopyTo(ms);
                        var fileContents = System.Text.Encoding.Default.GetString(ms.ToArray());
                        var blocksHexEncoded =
                            fileContents.Split(Environment.NewLine)
                            .Where(l => !string.IsNullOrEmpty(l));

                        return blocksHexEncoded;
                    }
                }
            }
        }
    }

    static class Label
    {
        public static string BLOCK = "block";
        public static string TRANSACTION = "tx";
        public static string TRANSACTION_OUT = "txout";
        public static string ADDRESS = "address";
        public static string PREVIOUS = "PREVIOUS";
        public static string SPENT = "SPENT";
        public static string MINED_IN = "MINED_IN";
        public static string LOCKED_AT = "LOCKED_AT";
        public static string PRODUCED = "PRODUCED";
    }
}
