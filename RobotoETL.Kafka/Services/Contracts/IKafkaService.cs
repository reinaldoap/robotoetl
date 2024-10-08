
namespace RobotoETL.Kafka.Services.Contracts
{
    public interface IKafkaService
    {
        Task ProduceAsync(string topic, string key, string value);

        void SetConsumer(IKafkaEventConsumerService consumerService);

    }
}
