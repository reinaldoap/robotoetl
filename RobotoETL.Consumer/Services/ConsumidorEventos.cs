using RobotoETL.Consumer.Services.Contracts;
using RobotoETL.Kafka.Services.Contracts;
using RobotoETL.Model;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

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
            Random random = new Random();

            _logger.LogInformation("Recebi uma lista com {total} pessoas para gerar prontuários médicos", events.Count);
            var prontuarios = new List<Prontuario>();

            foreach (var json in events) 
            {
                var pessoa = JsonSerializer.Deserialize<Pessoa>(json);
                if(pessoa == null)
                    continue;
                var prontuario = new Prontuario() { Paciente = pessoa };

                //Definindo a pressão cardiaca
                int number1 = random.Next(6, 21); 
                int number2 = random.Next(6, 21);

                // Encontrando o mínimo e o máximo
                prontuario.PressaoCardiacaMin = Math.Min(number1, number2);
                prontuario.PressaoCardiacaMax = Math.Max(number1, number2);

                prontuario.DataConsulta = DateTime.Now;

                prontuario.AvaliacaoMedica = $"O Paceinte {pessoa.Nome}, foi avaliado com a pressao {prontuario.PressaoCardiacaMin}/{prontuario.PressaoCardiacaMax} e está liberado para o trabalho";

                prontuarios.Add(prontuario);

                //Faz o envio para alguma api/servico dessa informação
            }


        }

    }
}
