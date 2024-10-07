

namespace RobotoETL.Kafka.Settings.Contracts
{
    internal interface IKafkaSettings
    {
        string GroupId { get; set; }
        string BootstrapServers { get; set; }
        string? AutoOffsetReset { get; set; }
        bool? EnableAutoCommit { get; set; }
        int? AutoCommitInterval { get; set; }

        bool HasConsumerProps { get; }
    }
}
