using RobotoETL.Producer.Services.Contracts;

namespace RobotoETL.Producer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IProdutorEventos _produtorEventos;

        public Worker(ILogger<Worker> logger, IProdutorEventos produtorEventos)
        {
            _logger = logger;
            _produtorEventos = produtorEventos;
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


            _logger.LogInformation("Iniciando a produção de eventos em: {time}", DateTimeOffset.Now);

            await _produtorEventos.ProduzirEventosAsync();

            _logger.LogInformation("Produção de eventos finalizada em: {time}", DateTimeOffset.Now);
        }
    }
}
