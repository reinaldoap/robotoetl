using RobotoETL.Kafka.Settings.Contracts;


namespace RobotoETL.Kafka.Settings
{
    internal class KafkaSettings : IKafkaSettings
    {
        public required string GroupId { get; set; }
        public required string BootstrapServers { get; set; }
        public string? AutoOffsetReset { get; set; }
        public bool HasConsumerProps => !string.IsNullOrWhiteSpace(AutoOffsetReset);
        public int EventsBatchSize { get; set; } = 100;
        public int TriggerBatchDelayMs { get; set; } = 30000;
    }
}
