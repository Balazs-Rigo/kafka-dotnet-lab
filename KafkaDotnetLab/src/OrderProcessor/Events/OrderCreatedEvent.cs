namespace OrderProcessor.Events;

public record OrderCreatedEvent(
    int? Version,
    Guid OrderId,
    string Product,
    DateTime CreatedAtUtc,
    decimal? Price);