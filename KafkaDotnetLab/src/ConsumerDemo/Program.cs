using Confluent.Kafka;
using Microsoft.Data.Sqlite;

namespace ConsumerDemo;

internal class Program
{
    private const string ConnectionString = "Data Source=consumer-demo.db";

    static void Main(string[] args)
    {
        InitializeDatabase();

        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "manual-commit-demo",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer =
            new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe("commit-demo");

        Console.WriteLine("Waiting for messages...");

        while (true)
        {
            var result = consumer.Consume();

            var messageId =
                $"{result.Topic}-{result.Partition.Value}-{result.Offset.Value}";

            Console.WriteLine();
            Console.WriteLine($"Received: {result.Message.Value}");
            Console.WriteLine($"MessageId: {messageId}");

            using var connection = new SqliteConnection(ConnectionString);
            connection.Open();

            using var transaction = connection.BeginTransaction();

            if (IsAlreadyProcessed(connection, transaction, messageId))
            {
                Console.WriteLine("Already processed -> skipping business logic.");

                transaction.Commit();

                consumer.Commit(result);

                Console.WriteLine("Offset committed.");

                continue;
            }

            Console.WriteLine("Executing business logic...");

            InsertOrder(
                connection,
                transaction,
                result.Message.Value);

            SaveProcessedMessage(
                connection,
                transaction,
                messageId);

            transaction.Commit();

            Console.WriteLine("DATABASE TRANSACTION COMMITTED");

            Console.WriteLine();
            Console.WriteLine("Press ENTER to commit Kafka offset.");
            Console.ReadLine();

            consumer.Commit(result);

            Console.WriteLine("Kafka offset committed.");
        }
    }

    private static void InitializeDatabase()
    {
        using var connection = new SqliteConnection(ConnectionString);
        connection.Open();

        var command = connection.CreateCommand();

        command.CommandText = """
            CREATE TABLE IF NOT EXISTS Orders
            (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                OrderNumber TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ProcessedMessages
            (
                MessageId TEXT PRIMARY KEY,
                ProcessedAtUtc TEXT NOT NULL
            );
            """;

        command.ExecuteNonQuery();
    }

    private static bool IsAlreadyProcessed(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string messageId)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;

        command.CommandText = """
            SELECT COUNT(*)
            FROM ProcessedMessages
            WHERE MessageId = $messageId;
            """;

        command.Parameters.AddWithValue("$messageId", messageId);

        var count = (long)command.ExecuteScalar()!;

        return count > 0;
    }

    private static void InsertOrder(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string orderNumber)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;

        command.CommandText = """
            INSERT INTO Orders (OrderNumber)
            VALUES ($orderNumber);
            """;

        command.Parameters.AddWithValue("$orderNumber", orderNumber);

        command.ExecuteNonQuery();

        Console.WriteLine($"Order inserted: {orderNumber}");
    }

    private static void SaveProcessedMessage(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string messageId)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;

        command.CommandText = """
            INSERT INTO ProcessedMessages
            (
                MessageId,
                ProcessedAtUtc
            )
            VALUES
            (
                $messageId,
                $processedAtUtc
            );
            """;

        command.Parameters.AddWithValue("$messageId", messageId);

        command.Parameters.AddWithValue(
            "$processedAtUtc",
            DateTime.UtcNow.ToString("O"));

        command.ExecuteNonQuery();

        Console.WriteLine($"Processed message saved: {messageId}");
    }
}