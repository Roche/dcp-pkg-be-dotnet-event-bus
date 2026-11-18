using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenDCP.Framework.Eventbus.Interfaces;
using OpenDCP.Framework.Messaging.Constants;
using OpenDCP.Framework.Messaging.Interfaces;

namespace OpenDCP.Framework.Eventbus
{
    public abstract class BaseEventBus : BackgroundService
    {
        protected readonly IServiceProvider ServiceProvider;
        protected readonly ILogger<BaseEventBus> Logger;
        protected readonly IMessageBus MessageBus;

        protected abstract string Module { get; }

        protected abstract int Version { get; }

        protected BaseEventBus(IServiceProvider serviceProvider, ILogger<BaseEventBus> logger, IMessageBus messageBus)
        {
            ServiceProvider = serviceProvider;
            Logger = logger;
            MessageBus = messageBus;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation("DCP {Module} Eventbus Start", Module);
            var quename = $"dcp.{Module.ToLower()}.v{Version}.eventbus";

            using (var scope = ServiceProvider.CreateScope())
            {
                var strategies = scope.ServiceProvider.GetServices<IMessageHandlerStrategy>();

                if (!strategies.Any())
                    throw new Exception("No strategies found - stopping service!");

                MessageBus.Connect($"{Module} EventBusWorkerService");
                MessageBus.InitializeQueue(quename);

                // Loop through every registered strategy dynamically
                foreach (var strategy in strategies)
                {
                    Logger.LogInformation(
                        "[{Module}] Binding queue for routing key: {RoutingKey}",
                        Module,
                        strategy.RoutingKey
                    );

                    // Perform your Message Bus Binding using the strategy's dynamic key
                    MessageBus.BindQueue(
                        quename,
                        MessageExchangers.EventBus,
                        new List<string> { strategy.RoutingKey }
                    );
                }
            }

            await base.StartAsync(cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            MessageBus.Disconnect();

            await base.StopAsync(cancellationToken);

            Logger.LogInformation("DCP {Module} Eventbus Stop", Module);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.ThrowIfCancellationRequested();

            await MessageBus.BindConsumerAsync(
                $"dcp.{Module.ToLower()}.v{Version}.eventbus",
                HandleMessageAsync
            );

            await Task.Yield();
        }

        private async Task HandleMessageAsync(string message, string type, string routingKey)
        {
            using var scope = ServiceProvider.CreateScope();
            var cts = new CancellationTokenSource();

            var strategies = scope.ServiceProvider.GetServices<IMessageHandlerStrategy>();

            var strategy = strategies.FirstOrDefault(s => s.RoutingKey.Equals(routingKey, StringComparison.OrdinalIgnoreCase));

            if (strategy != null)
            {
                // The abstract base class will do the deserialization heavy-lifting.
                await strategy.HandleMessageAsync(routingKey, message, cts.Token);
            }
            else
            {
                Logger.LogWarning("No handler registered for routing key: {RoutingKey}", routingKey);
            }
        }
    }
}
