using Confluent.Kafka.Serialization;

namespace Reactive.Kafka
{
    public interface IProducerBuilder<TKey, TValue>
    {
        IProducerBuilder<TKey, TValue> WithSettings(ProducerSettingsConfigurator settingsConfigurator);
        IProducerBuilder<TKey, TValue> WithKeySerializer(ISerializer<TKey> keySerializer);
        IProducerBuilder<TKey, TValue> WithValueSerializer(ISerializer<TValue> valueSerializer);
        IProducerBuilder<TKey, TValue> Targeting(string topic);
    }
}
