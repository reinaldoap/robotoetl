
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using RobotoETL.Kafka.Services.Contracts;
using RobotoETL.Kafka.Settings.Contracts;
using System.Threading;

namespace RobotoETL.Kafka.Services
{
    internal class KafkaService : IKafkaService
    {
        private readonly ILogger<KafkaService> _logger;
        private readonly IProducer<string, string> _producer;
        private IKafkaSettings _kafkaSettings;

        public KafkaService(ILogger<KafkaService> logger, IKafkaSettings kafkaSettings)
        {
            _logger = logger;
            _kafkaSettings = kafkaSettings;
            _producer = new ProducerBuilder<string, string>(
                 new ProducerConfig
                 {
                     BootstrapServers = kafkaSettings.BootstrapServers
                 }
            ).Build();
        }

        public async Task ProduceAsync(string topic, string key, string value)
        {
            try
            {
                var result = await _producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = key,
                    Value = value
                });

                _logger.LogInformation("Mensagem enviada para o topic {topic}", result.TopicPartitionOffset);
            }
            catch (ProduceException<string, string> e)
            {
                _logger.LogError("Falha ao enviar mensgem para {topic} com exception {exception}", topic, e.Message);
            }
        }

        public void SetConsumer(IKafkaEventConsumerService eventConsumer) 
        {
            if (eventConsumer == null)
            {
                _logger.LogError("Nenhum consumidor de eventos foi definido");
                return;
            }

            if (!_kafkaSettings.HasConsumerProps)
            {
                _logger.LogError("Configurações de consumer settings não definidas");
                return;
            }


            using (var consumer = new ConsumerBuilder<Ignore, string>(BuildConsumerConfig())
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine(
                        "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    Console.WriteLine(
                        "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build()) 
            {
                //List<ConsumeResult<Ignore, string>> batch = new List<ConsumeResult<Ignore, string>>();
                List<string> events = new();
                int batchSize = 100; // Number of messages in each batch

                consumer.Subscribe(eventConsumer.ConsumeTopic);


                try
                {
                    while (true)
                    {
                        try
                        {
                            var cancellationTokenSource = new CancellationTokenSource();
                            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(5)); // Consume for 5 seconds


                            var consumeResult = consumer.Consume(cancellationTokenSource.Token);

                            if (consumeResult.IsPartitionEOF) //Chegou no final da partição
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                            events.Add(consumeResult.Message.Value);

                            // Check if batch is full
                            if (events.Count >= batchSize)
                            {
                                // Process the batch of messages
                                Console.WriteLine($"Processing batch of {events.Count} messages");
                                eventConsumer.OnConsume(events);
                                consumer.Commit(); //.StoreOffset(consumeResult);
                                events.Clear();
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }

            }
        }


        private ConsumerConfig BuildConsumerConfig() 
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _kafkaSettings.BootstrapServers,
                GroupId = _kafkaSettings.GroupId,
                EnableAutoOffsetStore = true,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            return config;
        }
    }
}
