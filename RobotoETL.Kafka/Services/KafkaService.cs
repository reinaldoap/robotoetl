
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using RobotoETL.Kafka.Services.Contracts;
using RobotoETL.Kafka.Settings.Contracts;

namespace RobotoETL.Kafka.Services
{
    internal class KafkaService : IKafkaService
    {
        private readonly ILogger<KafkaService> _logger;
        private readonly IProducer<string, string> _producer;
        private IKafkaSettings _kafkaSettings;

        private IConsumer<Ignore, string>? _consumer;
        private IKafkaEventConsumerService? _eventConsumerService;
        private readonly List<string> _events = new List<string>();


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

        public void SetConsumer(IKafkaEventConsumerService eventConsumerService) 
        {
            if (eventConsumerService == null)
            {
                _logger.LogError("Nenhum consumidor de eventos foi definido");
                return;
            }

            if (!_kafkaSettings.HasConsumerProps)
            {
                _logger.LogError("Configurações de consumer settings não definidas");
                return;
            }

            _eventConsumerService = eventConsumerService;


            using (_consumer = new ConsumerBuilder<Ignore, string>(BuildConsumerConfig())
                // Note: All handlers are called on the main .Consume thread.
                .SetErrorHandler((_, e) => _logger.LogError("Erro Kafka, {reason}", e.Reason))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));

                    var particoesAtribuidas = string.Join(',', partitions.Select(p => p.Partition.Value));
                    var todasParticoes = string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value));
                    _logger.LogDebug("Partições atribuidas incrementalmente: [{particoesAtribuidas}], todas as particoes: [{todasParticoes}]", particoesAtribuidas, todasParticoes);

                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    var revokedPartitions = partitions.Select(p => p.Partition.Value);
                    var remainingPartitions  = remaining.Select(p => p.Partition.Value);
                    _logger.LogDebug("Particoes incrementalmente revogadas: [{revokedPartitions}], particoes restantes: [{remaningPartitions}]", revokedPartitions, remainingPartitions);
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    var lotstPartitions = string.Join(", ", partitions);
                    _logger.LogDebug("Particoes foram perdidas: [{lostPartitions}]", lotstPartitions);
                })
                .Build()) 
            {
                
                int batchSize = 100; // Number of messages in each batch

                _consumer.Subscribe(_eventConsumerService.ConsumeTopic);

                var cancellationTokenSource = new CancellationTokenSource();


                //Permite encerrar a aplicação com o Ctrl + C
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // Prevent the process from terminating immediately
                    cancellationTokenSource.Cancel(); // Signal cancellation to stop consuming
                };

                var cts = new CancellationTokenSource();

                try
                {
                    // Utilizando o cancelation Token no while consigo parar a aplicação com o Ctrl+C ou programaticalmente.
                    while (!cancellationTokenSource.Token.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = _consumer.Consume();
                            cts.Cancel(); //Cancela a execução anterior do 'ProcessConsumerEvent()'
                            cts.Dispose();
                            cts = new CancellationTokenSource();


                            if (consumeResult.IsPartitionEOF) //Chegou no final da partição
                            {
                                _logger.LogDebug("Reached end of topic {topic}, partition {partition}, offset {Offset}.", 
                                    consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

                                // Cheguei ao final do tópico, se houver itens pendente na lista de events
                                // faz a execução como se tivesse atingido o limite do lote
                                _ = ProcessConsumerEventAsync(30000, cts.Token); //Não coloco await para não travar a Thread.
                                continue;
                            }

                            _logger.LogInformation("Received message at {TopicPartitionOffset}: {Value}",  consumeResult.TopicPartitionOffset, consumeResult.Message.Value);
                            _events.Add(consumeResult.Message.Value);

                            // Verifica se atingiu o tamanho máximo do lote
                            if (_events.Count >= batchSize)
                                ProcessConsumerEventAsync(0, CancellationToken.None).Wait(); //Executa o método de imediato
                        }
                        catch (ConsumeException e)
                        {
                            _logger.LogError("Falha ao consumir kafka evento | {reason}", e.Error.Reason);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogError("Kafka OperationCanceledException, fechando o consumer.");
                    _consumer.Close();
                }
            }
        }

        /// <summary>
        ///  Faz o serviço de consumer executar o que deve ser feito e em caso de falha nessa execução
        ///  vai lançar uma execption caindo no catch do ConsumerException
        ///  não efetuando o commit.
        ///  
        ///  delayInMilliseconds, usado em caso a aplicação fique muito tempo sem receber msg, 
        ///  dessa maneira não fica msg parada no array de events
        ///  
        ///  token, caso eu receba novos eventos posso efetuar a pausa dessa execução.
        /// </summary>
        private async Task ProcessConsumerEventAsync(int delayInMilliseconds, CancellationToken token) 
        {
            try
            {
                await Task.Delay(delayInMilliseconds, token); // Wait for delay or cancellation
                if (!token.IsCancellationRequested)
                {
                    if (_eventConsumerService == null || _consumer == null)
                        return;

                    // Não posso fazer commit se não tiver feito a leitura de alguma mensagem
                    // caso contrário gera uma Exception "Confluent.Kafka.KafkaException: 'Local: No offset stored'"
                    // portanto devo veriricar o array de eventos antes de efetuar o commit
                    if (_events.Any()) 
                    {
                        _eventConsumerService.OnConsume(_events);
                        _consumer.Commit();
                        _events.Clear();
                    }
                }
            }
            catch (TaskCanceledException)
            {
                _logger.LogDebug("Temporizador cancelado antes de engatilhar o evento.");
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
