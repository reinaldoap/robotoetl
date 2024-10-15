using RobotoETL.Consumer.Services.Contracts;

namespace RobotoETL.Consumer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IPacientesConsumerService _consumidorEventos;

        public Worker(ILogger<Worker> logger, IPacientesConsumerService consumidorEventos)
        {
            _logger = logger;
            _consumidorEventos = consumidorEventos;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Iniciando o consumo de eventos em: {time}", DateTimeOffset.Now);
            await _consumidorEventos.OuvirPacientesAsync();
        }
    }
}
