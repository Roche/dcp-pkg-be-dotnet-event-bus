namespace OpenDCP.Framework.Eventbus.Interfaces
{
    public interface IMessageHandlerStrategy
    {
        public string RoutingKey { get; }
        public Task HandleMessageAsync(string routingKey, string rawPayload, CancellationToken cancellationToken);
    }
}
