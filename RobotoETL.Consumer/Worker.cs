using RobotoETL.Consumer.Services.Contracts;

namespace RobotoETL.Consumer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConsumidorEventos _consumidorEventos;

        public Worker(ILogger<Worker> logger, IConsumidorEventos consumidorEventos)
        {
            _logger = logger;
            _consumidorEventos = consumidorEventos;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            /*while (!stoppingToken.IsCancellationRequested)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                }
                await Task.Delay(1000, stoppingToken);
            }*/

            await _consumidorEventos.ConsumirEventosAsync();
        }
    }
}
