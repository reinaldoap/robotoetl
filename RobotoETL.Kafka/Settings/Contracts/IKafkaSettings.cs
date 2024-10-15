

namespace RobotoETL.Kafka.Settings.Contracts
{
    internal interface IKafkaSettings
    {
        string GroupId { get; set; }
        string BootstrapServers { get; set; }
        string? AutoOffsetReset { get; set; }
        bool HasConsumerProps { get; }
        int EventsBatchSize { get; set; }
        int TriggerBatchDelayMs { get; set; }
        
    }
}
