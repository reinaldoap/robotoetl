using RobotoETL.Consumer;
using RobotoETL.Consumer.Services;
using RobotoETL.Consumer.Services.Contracts;
using RobotoETL.Kafka.Extensions;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

//Adiciona os servi�os do Kafka para o consumer
builder.Services.AddKafkaServices(builder.Configuration);

//Servi�os do consumidor
builder.Services.AddSingleton<IConsumidorEventos, ConsumidorEventos>();

var host = builder.Build();
host.Run();
