using Confluent.Kafka.Serialization;

namespace Reactive.Kafka
{
    public interface IConsumerBuilder<TKey, TValue>
    {
        IConsumerBuilder<TKey, TValue> WithSettings(ConsumerSettingsConfigurator settingsConfigurator);
        IConsumerBuilder<TKey, TValue> SubscribedTo(string topic);
        IConsumerBuilder<TKey, TValue> WithKeyDeserializer(IDeserializer<TKey> keyDeserializer);
        IConsumerBuilder<TKey, TValue> WithValueDeserializer(IDeserializer<TValue> valueDeserializer);
        
        
    }
}
