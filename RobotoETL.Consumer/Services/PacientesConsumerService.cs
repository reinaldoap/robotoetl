using RobotoETL.Consumer.Services.Contracts;
using RobotoETL.Consumer.Settings;
using RobotoETL.Kafka.Services.Contracts;
using RobotoETL.Model;
using System;
using System.Text.Json;

namespace RobotoETL.Consumer.Services
{
    internal class PacientesConsumerService : IPacientesConsumerService, IKafkaEventConsumerService
    {
        private readonly ILogger<PacientesConsumerService> _logger;
        private readonly IKafkaService _kafkaService;
        private readonly PacientesConsumerSettings _settings;

        public PacientesConsumerService(ILogger<PacientesConsumerService> logger, IKafkaService kafkaService,
            PacientesConsumerSettings settings)
        {
            _logger = logger;
            _kafkaService = kafkaService;
            _settings = settings;
            
        }

        public string ConsumeTopic => _settings.Topico;

        public Task OuvirPacientesAsync()
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
            

            _logger.LogInformation("Recebi uma lista com {total} pessoas para gerar prontuários médicos", events.Count);
            var prontuarios = new List<Prontuario>();

            foreach (var json in events) 
            {
                var pessoa = JsonSerializer.Deserialize<Pessoa>(json);
                if(pessoa == null)
                    continue;
                var prontuario = new Prontuario() { Paciente = pessoa };

                prontuario.Cardio().Imc();
                prontuario.DataConsulta = DateTime.Now;
                prontuario.AvaliacaoMedica = $"O paciente {pessoa.Nome}, foi avaliado com a pressao " + 
                            $" {prontuario.PressaoCardiacaMin}/{prontuario.PressaoCardiacaMax} e {prontuario.ResultadoImc}." +
                            " estando portando liberado para o trabalho ";




                _logger.LogInformation(JsonSerializer.Serialize(prontuario));
                prontuarios.Add(prontuario);

                //Faz o envio para alguma api/servico dessa informação
            }
        }

        #region [Calculos de saude]


        #endregion

    }
}
