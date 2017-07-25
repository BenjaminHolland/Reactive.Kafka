namespace Reactive.Kafka
{
    public interface IProducerSettingsBuilder
    {
        IProducerSettingsBuilder AddBroker(string host);
    }
}
