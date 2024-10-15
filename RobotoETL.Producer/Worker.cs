using RobotoETL.Producer.Services.Contracts;

namespace RobotoETL.Producer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IPacientesProducerService _pacientesProducerService;

        public Worker(ILogger<Worker> logger, IPacientesProducerService pacientesProducerService)
        {
            _logger = logger;
            _pacientesProducerService = pacientesProducerService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Iniciando a produção de eventos em: {time}", DateTimeOffset.Now);
                await _pacientesProducerService.EmitirPacientesAsync();
                _logger.LogInformation("Produção de eventos finalizada em: {time}", DateTimeOffset.Now);
                await Task.Delay(TimeSpan.FromMinutes(2.5), stoppingToken);
            }
           
        }
    }
}
