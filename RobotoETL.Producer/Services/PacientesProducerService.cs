using Bogus;
using Bogus.Extensions.Brazil;
using RobotoETL.Kafka.Services.Contracts;
using RobotoETL.Model;
using RobotoETL.Producer.Services.Contracts;
using RobotoETL.Producer.Settings;
using System.Text.Json;

namespace RobotoETL.Producer.Services
{
    internal class PacientesProducerService : IPacientesProducerService
    {
        private readonly ILogger<PacientesProducerService> _logger;
        private readonly Faker<Pessoa> _faker;
        private readonly IKafkaService _kafkaService;
        private readonly PacientesProducerSettings _settings;

        public PacientesProducerService(ILogger<PacientesProducerService> logger, 
            IKafkaService kafkaService, PacientesProducerSettings settings)
        {
            _logger = logger;
            _kafkaService = kafkaService;
            _settings = settings;
            _faker = BuildFaker();
        }

        private Faker<Pessoa> BuildFaker() 
        {
            var fk = new Faker<Pessoa>("pt_BR");

            fk.RuleFor(p => p.Sexo, f => f.PickRandom<Sexo>());
            fk.RuleFor(p => p.Cpf, f => f.Person.Cpf());
            fk.RuleFor(p => p.Nome, (f, p) => f.Person.FullName);
            fk.RuleFor(p => p.DataNascimento, (f, p) => f.Person.DateOfBirth);

            return fk;
        
        }


        public async Task EmitirPacientesAsync()
        {
            for(var i =0; i < _settings.Quantidade; i++) 
            {
                var pessoa = _faker.Generate();
                var pessoaJson = JsonSerializer.Serialize(pessoa);
                _logger.LogInformation("Emitindo evento para: {Nome}", pessoa.Nome);

                await _kafkaService.ProduceAsync(_settings.Topico, pessoa.Cpf, pessoaJson);
                
            }
        }
    }
}
