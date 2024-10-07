using RobotoETL.Kafka.Settings.Contracts;


namespace RobotoETL.Kafka.Settings
{
    internal class KafkaSettings : IKafkaSettings
    {
        public required string GroupId { get; set; }
        public required string BootstrapServers { get; set; }
        public string? AutoOffsetReset { get; set; }
        public bool? EnableAutoCommit { get; set; }
        public int? AutoCommitInterval { get; set; }

        public bool HasConsumerProps => !string.IsNullOrWhiteSpace(AutoOffsetReset) && EnableAutoCommit.HasValue;
    }
}
