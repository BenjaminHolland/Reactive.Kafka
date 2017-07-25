using System;

namespace Reactive.Kafka
{
    public interface IConsumerSettingsBuilder
    {
        IConsumerSettingsBuilder AddBroker(string host);
        IConsumerSettingsBuilder WithGroupId(string groupId);
        IConsumerSettingsBuilder WithClientId(string clientId);
        IConsumerSettingsBuilder WithSessionTimeout(TimeSpan sessionTimeout);
        IConsumerSettingsBuilder WithCustomSetting(string settingName, object settingValue);
        
    }
}
