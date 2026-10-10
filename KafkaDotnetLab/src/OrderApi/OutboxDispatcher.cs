using Confluent.Kafka;
using Microsoft.Data.Sqlite;

namespace OrderApi;

public class OutboxDispatcher : BackgroundService
{
    private readonly ILogger<OutboxDispatcher> _logger;
    private readonly IConfiguration _configuration;
    private readonly IProducer<string, string> _producer;

    public OutboxDispatcher(
        ILogger<OutboxDispatcher> logger,
        IConfiguration configuration,
        IProducer<string, string> producer)
    {
        _logger = logger;
        _configuration = configuration;
        _producer = producer;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var connectionString =
            _configuration.GetConnectionString("OrderDatabase")
            ?? throw new InvalidOperationException(
                "OrderDatabase connection string is missing.");

        while (!stoppingToken.IsCancellationRequested)
        {
            using var connection =
                new SqliteConnection(connectionString);

            await connection.OpenAsync(stoppingToken);

            var command = connection.CreateCommand();

            command.CommandText = """
                SELECT Id, EventType, Payload
                FROM OutboxMessages
                WHERE ProcessedAtUtc IS NULL
                ORDER BY CreatedAtUtc
                LIMIT 10;
                """;

            using var reader =
                await command.ExecuteReaderAsync(stoppingToken);

            var messages = new List<OutboxMessage>();

            while (await reader.ReadAsync(stoppingToken))
            {
                messages.Add(new OutboxMessage(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2)));
            }

            foreach (var message in messages)
            {
                _logger.LogInformation(
                    "Publishing outbox message {Id}",
                    message.Id);

                await _producer.ProduceAsync(
                    "orders",
                    new Message<string, string>
                    {
                        Key = message.Id,
                        Value = message.Payload
                    },
                    stoppingToken);

                var updateCommand =
                    connection.CreateCommand();

                updateCommand.CommandText = """
                    UPDATE OutboxMessages
                    SET ProcessedAtUtc = $processedAtUtc
                    WHERE Id = $id;
                    """;

                updateCommand.Parameters.AddWithValue(
                    "$processedAtUtc",
                    DateTime.UtcNow.ToString("O"));

                updateCommand.Parameters.AddWithValue(
                    "$id",
                    message.Id);

                await updateCommand.ExecuteNonQueryAsync(
                    stoppingToken);

                _logger.LogInformation(
                    "Outbox message {Id} published and marked processed.",
                    message.Id);
            }

            await Task.Delay(
                TimeSpan.FromSeconds(2),
                stoppingToken);
        }
    }

    private sealed record OutboxMessage(
        string Id,
        string EventType,
        string Payload);
}