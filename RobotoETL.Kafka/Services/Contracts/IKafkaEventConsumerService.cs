namespace RobotoETL.Kafka.Services.Contracts
{
    public interface IKafkaEventConsumerService
    {
        string ConsumeTopic { get; }
        void OnConsume(List<string> events);
    }
}
