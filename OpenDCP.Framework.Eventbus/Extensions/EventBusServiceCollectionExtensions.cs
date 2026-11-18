using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenDCP.Framework.Eventbus.Interfaces;
using System.Reflection;

namespace OpenDCP.Framework.Eventbus.Extensions
{
    public static class EventBusServiceCollectionExtensions
    {
        public static IServiceCollection AddMessagingWorker<TWorker>(
            this IServiceCollection services,
            Assembly assemblyWithStrategies)
            where TWorker : BackgroundService
        {

            services.AddHostedService<TWorker>();

            // Scan the provided assembly for all IMessageHandlerStrategy implementations
            var strategyType = typeof(IMessageHandlerStrategy);
            var implementations = assemblyWithStrategies.GetTypes()
                .Where(p => strategyType.IsAssignableFrom(p) && !p.IsInterface && !p.IsAbstract);

            foreach (var type in implementations)
            {
                // Register them as Scoped so they can safely use database contexts/repositories
                services.AddScoped(typeof(IMessageHandlerStrategy), type);
            }

            return services;
        }
    }
}
