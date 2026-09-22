using Confluent.Kafka;

namespace OrderProcessor;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConsumer<string, string> _consumer;

    public Worker(
        ILogger<Worker> logger,
        IConsumer<string, string> consumer)
    {
        _logger = logger;
        _consumer = consumer;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe("orders");

        _logger.LogInformation("Order consumer started.");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = _consumer.Consume(stoppingToken);

                _logger.LogInformation(
                    "Received order. Key={Key}, Value={Value}, Partition={Partition}, Offset={Offset}",
                    result.Message.Key,
                    result.Message.Value,
                    result.Partition.Value,
                    result.Offset.Value);

                // Business logic helye

                _consumer.Commit(result);

                _logger.LogInformation("Offset committed.");
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer stopping...");
        }
        finally
        {
            _consumer.Close();
        }

        return Task.CompletedTask;
    }
}