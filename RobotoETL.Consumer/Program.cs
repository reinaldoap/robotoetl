using Microsoft.Extensions.Options;
using RobotoETL.Consumer;
using RobotoETL.Consumer.Services;
using RobotoETL.Consumer.Services.Contracts;
using RobotoETL.Consumer.Settings;
using RobotoETL.Kafka.Extensions;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

//Adiciona os serviços do Kafka para o consumer
builder.Services.AddKafkaServices(builder.Configuration);

// Configuração do consumer
builder.Services.Configure<PacientesConsumerSettings>(builder.Configuration.GetSection(nameof(PacientesConsumerSettings)));
builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<IOptions<PacientesConsumerSettings>>().Value
);


//Serviços do consumidor
builder.Services.AddSingleton<IPacientesConsumerService, PacientesConsumerService>();

var host = builder.Build();
host.Run();
