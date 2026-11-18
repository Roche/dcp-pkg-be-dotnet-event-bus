using Microsoft.Extensions.Logging;
using OpenDCP.Framework.Connections.Core;
using OpenDCP.Framework.Eventbus.Interfaces;
using System.Text.Json;

namespace OpenDCP.Framework.Eventbus.Strategies
{
    public abstract class BaseMessageHandlerStrategy<TMessage> : IMessageHandlerStrategy
    {
        protected readonly IDCPCoreConnection _coreConnection;
        protected readonly ILogger _logger;
        protected BaseMessageHandlerStrategy(IDCPCoreConnection coreConnection, ILoggerFactory loggerFactory)
        {
            _coreConnection = coreConnection;
            _logger = loggerFactory.CreateLogger(GetType());
        }

        public abstract string RoutingKey { get; }

        public async Task HandleMessageAsync(string routingKey, string rawPayload, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Processing message for routing key: '{RoutingKey}'", routingKey);
            _logger.LogDebug("Recived message is: '{messgae}'", rawPayload);

            // Automatically deserialize the raw string to a custom type TMessage
            var deserializedMessage = JsonSerializer.Deserialize<TMessage>(
                rawPayload,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true }
            );

            if (deserializedMessage == null)
            {
                throw new InvalidOperationException($"Failed to deserialize payload for key '{RoutingKey}' into {typeof(TMessage).Name}");
            }

            // Hand off the strongly-typed object to domain logic
            await HandleAsync(deserializedMessage, cancellationToken);
        }

        // This is the method a derrived Event Bus should implement
        protected abstract Task HandleAsync(TMessage message, CancellationToken cancellationToken);
    }
}
