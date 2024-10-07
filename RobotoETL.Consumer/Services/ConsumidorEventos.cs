using RobotoETL.Consumer.Services.Contracts;
using RobotoETL.Kafka.Services.Contracts;

namespace RobotoETL.Consumer.Services
{
    internal class ConsumidorEventos : IConsumidorEventos, IKafkaEventConsumerService
    {
        private readonly ILogger<ConsumidorEventos> _logger;
        private readonly IKafkaService _kafkaService;

        public ConsumidorEventos(ILogger<ConsumidorEventos> logger, IKafkaService kafkaService)
        {
            _logger = logger;
            _kafkaService = kafkaService;
            
        }

        public string ConsumeTopic => "topic-pessoas";

        public Task ConsumirEventosAsync()
        {
            _logger.LogInformation("Iniciando o consumo de eventos...");

            _kafkaService.SetConsumer(this);

            return Task.CompletedTask;
        }

        /**
        * Implementa a interface IKafkaEventConsumerService, nesse método
        * eu devo fazer alguma ação com o lote recebido do kafka, se esse método 
        * lança alguma exeção a lib RobotoETL.Kafka vai ser capaz de tratar a falha
        * e não comitar as msgs
        */
        public void OnConsume(List<string> events)
        {
            _logger.LogInformation("Recebi uma lista com {total} itens para fazer alguma coisa", events.Count);
        }

    }
}
