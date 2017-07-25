namespace Reactive.Kafka
{
    public interface IConsumerSettingsBuilder
    {
        IConsumerSettingsBuilder AddBroker(string host);
        IConsumerSettingsBuilder WithGroupId(string groupId);
    }
}
