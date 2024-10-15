using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using RobotoETL.Kafka.Extensions;
using RobotoETL.Producer;
using RobotoETL.Producer.Services;
using RobotoETL.Producer.Services.Contracts;
using RobotoETL.Producer.Settings;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();


//Adiciona os serviços do Kafka para o producer
builder.Services.AddKafkaServices(builder.Configuration);

// Configuração do producer
builder.Services.Configure<PacientesProducerSettings>(builder.Configuration.GetSection(nameof(PacientesProducerSettings)));
builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<IOptions<PacientesProducerSettings>>().Value
);

//Serviços do produtor
builder.Services.AddSingleton<IPacientesProducerService, PacientesProducerService>();


var host = builder.Build();
host.Run();
