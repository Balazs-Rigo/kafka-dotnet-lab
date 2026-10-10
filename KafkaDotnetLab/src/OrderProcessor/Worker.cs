using Confluent.Kafka;
using System.Text.Json;
using OrderProcessor.Events;

namespace OrderProcessor;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly IProducer<string, string> _producer;

    public Worker(ILogger<Worker> logger, IConsumer<string, string> consumer, IProducer<string, string> producer)
    {
        _logger = logger;
        _consumer = consumer;
        _producer = producer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe("orders");

        _logger.LogInformation("Order consumer started.");

        var processedCount = 0;
        var throughputStartedAt = DateTime.UtcNow;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Waiting for Kafka message...");

                var result = _consumer.Consume(stoppingToken);

                _logger.LogInformation(
                    "Kafka message received. Topic={Topic}, Partition={Partition}, Offset={Offset}, Key={Key}",
                    result.Topic,
                    result.Partition.Value,
                    result.Offset.Value,
                    result.Message.Key);

                OrderCreatedEvent? order;

                try
                {
                    order = JsonSerializer.Deserialize<OrderCreatedEvent>(
                        result.Message.Value);

                    if (order is null)
                    {
                        _logger.LogWarning("Could not deserialize order event.");

                        _consumer.Commit(result);
                        continue;
                    }

                    _logger.LogInformation(
                        "Deserialized order. Version={Version}, OrderId={OrderId}, Product={Product}, CreatedAtUtc={CreatedAtUtc}, Price={Price}",
                        order.Version,
                        order.OrderId,
                        order.Product,
                        order.CreatedAtUtc,
                        order.Price);
                }
                catch (JsonException ex)
                {
                    _logger.LogError(ex,
                        "Invalid message format. Sending to DLQ. Topic={Topic}, Partition={Partition}, Offset={Offset}, Key={Key}",
                        result.Topic,
                        result.Partition.Value,
                        result.Offset.Value,
                        result.Message.Key);

                    await _producer.ProduceAsync(
                        "orders-dlq",
                        new Message<string, string>
                        {
                            Key = result.Message.Key,
                            Value = result.Message.Value
                        },
                        stoppingToken);

                    _logger.LogInformation("Invalid message sent to DLQ.");

                    _consumer.Commit(result);

                    _logger.LogInformation("Kafka offset committed. Topic={Topic}, Partition={Partition}, Offset={Offset}",
                        result.Topic,
                        result.Partition.Value,
                        result.Offset.Value);

                    continue;
                }

                var startedAt = DateTime.UtcNow;

                _logger.LogInformation("Processing order...");

                Thread.Sleep(3000);

                _logger.LogInformation("Business logic finished.");

                var processingTime = DateTime.UtcNow - startedAt;

                _logger.LogInformation(
                    "Order processing completed in {ProcessingMs} ms. OrderId={OrderId}",
                    processingTime.TotalMilliseconds,
                    order.OrderId);

                _logger.LogInformation(
                    "Order processed successfully. OrderId={OrderId}",
                    order.OrderId);

                processedCount++;

                var elapsed = DateTime.UtcNow - throughputStartedAt;

                if (elapsed.TotalSeconds >= 10)
                {
                    var messagesPerSecond = processedCount / elapsed.TotalSeconds;

                    _logger.LogInformation(
                        "Consumer throughput: {MessagesPerSecond:F2} msg/sec. Processed={ProcessedCount}, WindowSeconds={WindowSeconds:F1}",
                        messagesPerSecond,
                        processedCount,
                        elapsed.TotalSeconds);

                    processedCount = 0;
                    throughputStartedAt = DateTime.UtcNow;
                }

                _consumer.Commit(result);

                _logger.LogInformation(
                    "Kafka offset committed. Topic={Topic}, Partition={Partition}, Offset={Offset}",
                    result.Topic,
                    result.Partition.Value,
                    result.Offset.Value);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Cancellation requested. Consumer loop stopping...");
        }
        finally
        {
            _logger.LogInformation("Closing Kafka consumer...");

            _consumer.Close();

            _logger.LogInformation("Kafka consumer closed.");
        }
    }
}