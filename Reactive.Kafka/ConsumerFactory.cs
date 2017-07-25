using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace Reactive.Kafka
{

    public delegate void ConsumerSettingsConfigurator(IConsumerSettingsBuilder builder);
    public delegate void ProducerSettingsConfigurator(IProducerSettingsBuilder builder);
    public delegate void ConsumerConfigurator<TKey, TValue>(IConsumerBuilder<TKey, TValue> builder);
    public delegate void ProducerConfigurator<TKey, TValue>(IProducerBuilder<TKey, TValue> builder);

    public static class ConsumerFactory
    {
        private sealed class ConsumerBuilder<TKey, TValue> : IConsumerBuilder<TKey, TValue>
        {
            private sealed class ConsumerSettingsBuilder : IConsumerSettingsBuilder
            {
                private readonly ISet<string> _brokers = new HashSet<string>();
                private string _groupId;
                TimeSpan? _sessionTimeout;
                public IConsumerSettingsBuilder AddBroker(string host)
                {
                    _brokers.Add(host);
                    return this;
                }

                public IConsumerSettingsBuilder WithGroupId(string groupId)
                {
                    _groupId = groupId;
                    return this;
                }
                public IReadOnlyDictionary<string, object> Build()
                {
                    Dictionary<string, object> settings = new Dictionary<string, object>();
                    if (_brokers.Count < 1) throw new InvalidOperationException("Consumer requires at least one broker.");
                    settings.Add("bootstrap.servers", String.Join(",", _brokers));
                    if (_groupId == null) throw new InvalidOperationException("Consumer must have a group id.");
                    if (_sessionTimeout.HasValue)
                    {
                        int timeout_ms = checked((int)_sessionTimeout.Value.TotalMilliseconds);
                        if (timeout_ms< 0) throw new NotSupportedException("Session timeout cannot be negative.");
                        settings.Add("session.timeout.ms", timeout_ms);
                    }
                    settings.Add("group.id", _groupId);
                    return settings;
                }

                public IConsumerSettingsBuilder WithSessionTimeout(TimeSpan sessionTimeout)
                {
                    _sessionTimeout = sessionTimeout;
                    return this;
                }
            }
            private readonly ISet<string> _topics = new HashSet<string>();
            private IDeserializer<TKey> _ks;
            private IDeserializer<TValue> _vs;
            private ConsumerSettingsConfigurator _settingsCfg;

            public IConsumerBuilder<TKey, TValue> SubscribedTo(string topic)
            {
                _topics.Add(topic);
                return this;
            }

            public IConsumerBuilder<TKey, TValue> WithKeyDeserializer(IDeserializer<TKey> keyDeserializer)
            {
                _ks = keyDeserializer;
                return this;
            }

            public IConsumerBuilder<TKey, TValue> WithSettings(ConsumerSettingsConfigurator settingsConfigurator)
            {
                _settingsCfg = settingsConfigurator;
                return this;
            }

            public IConsumerBuilder<TKey, TValue> WithValueDeserializer(IDeserializer<TValue> valueDeserializer)
            {
                _vs = valueDeserializer;
                return this;
            }
            public Consumer<TKey, TValue> Build()
            {
                if (_settingsCfg == null) throw new InvalidOperationException("Settings configurator required.");
                var bldr = new ConsumerSettingsBuilder();
                _settingsCfg(bldr);
                if (_topics.Count < 1) throw new InvalidOperationException("At least one topic required.");
                Consumer<TKey, TValue> ret = new Consumer<TKey, TValue>(bldr.Build(), _ks, _vs);
                ret.Subscribe(_topics);
                return ret;
            }
        }
        public static Consumer<TKey, TValue> CreateConsumer<TKey, TValue>(ConsumerConfigurator<TKey, TValue> consumerConfigurator)
        {
            if (consumerConfigurator == null) throw new InvalidOperationException("Consumer configurator required.");
            var bldr = new ConsumerBuilder<TKey, TValue>();
            consumerConfigurator(bldr);
            return bldr.Build();
        }
        private static void RunPollingLoop<TKey, TValue>(Consumer<TKey, TValue> consumer, CancellationToken ct, IObserver<Message<TKey, TValue>> obs)
        {
            if (!ct.IsCancellationRequested)
            {
                consumer.OnConsumeError += (s, e) => obs.OnError(new KafkaException(e.Error));
                consumer.OnError += (s, e) => obs.OnError(new KafkaException(e));
                consumer.OnMessage += (s, e) => obs.OnNext(e);
                while (!ct.IsCancellationRequested)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        private static IObservable<Message<TKey, TValue>> AsObservable<TKey, TValue>(this Consumer<TKey, TValue> consumer)
        {
            if (consumer == null) throw new ArgumentNullException(nameof(consumer));
            return Observable.Create<Message<TKey, TValue>>((obs) =>
            {
                var cts = new CancellationTokenSource();
                Task.Run(() => RunPollingLoop(consumer, cts.Token, obs));
                return Disposable.Create(() =>
                {
                    cts.Cancel();
                });
            });
        }

        public static IConnectableObservable<Message<TKey, TValue>> CreateObservable<TKey, TValue>(ConsumerConfigurator<TKey, TValue> consumerConfigurator) => Observable.
            Using(() => CreateConsumer(consumerConfigurator), (consumer) => consumer.AsObservable()).
            Publish();

    }
}
