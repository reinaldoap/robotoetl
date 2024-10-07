
using Microsoft.Extensions.DependencyInjection;
using RobotoETL.Kafka.Settings.Contracts;
using RobotoETL.Kafka.Settings;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using RobotoETL.Kafka.Services.Contracts;
using RobotoETL.Kafka.Services;

namespace RobotoETL.Kafka.Extensions
{
    public static class ServicesCollectionExtensions
    {
        public static IServiceCollection AddKafkaServices(this IServiceCollection services, IConfigurationRoot configuration) 
        {
            services.Configure<KafkaSettings>(configuration.GetSection("KafkaSettings"));
            services.AddSingleton<IKafkaSettings>(sp =>
                sp.GetRequiredService<IOptions<KafkaSettings>>().Value
            );

            services.AddSingleton<IKafkaService, KafkaService>();

            return services;
        }
    }
}
