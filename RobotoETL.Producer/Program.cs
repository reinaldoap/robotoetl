using RobotoETL.Kafka.Extensions;
using RobotoETL.Producer;
using RobotoETL.Producer.Services;
using RobotoETL.Producer.Services.Contracts;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();


//Adiciona os serviços do Kafka para o producer
builder.Services.AddKafkaServices(builder.Configuration);


//Serviços do produtor
builder.Services.AddSingleton<IProdutorEventos, ProdutorEventos>();


var host = builder.Build();
host.Run();
