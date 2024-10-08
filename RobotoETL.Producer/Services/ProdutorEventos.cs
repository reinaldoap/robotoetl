using Bogus;
using Bogus.Extensions.Brazil;
using RobotoETL.Kafka.Services.Contracts;
using RobotoETL.Model;
using RobotoETL.Producer.Services.Contracts;
using System;
using System.Text.Json;

namespace RobotoETL.Producer.Services
{
    internal class ProdutorEventos : IProdutorEventos
    {
        private readonly ILogger<ProdutorEventos> _logger;
        private readonly Faker<Pessoa> _faker;
        private readonly IKafkaService _kafkaService;
        public ProdutorEventos(ILogger<ProdutorEventos> logger, IKafkaService kafkaService)
        {
            _logger = logger;
            _kafkaService = kafkaService;
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


        public async Task ProduzirEventosAsync()
        {
            for(var i =0; i < 1000; i++) 
            {
                var pessoa = _faker.Generate();
                var pessoaJson = JsonSerializer.Serialize(pessoa);
                _logger.LogInformation("Emitindo evento para: {Nome}", pessoa.Nome);

                await _kafkaService.ProduceAsync("topic-pessoas", pessoa.Cpf, pessoaJson);
                
            }
        }
    }
}
