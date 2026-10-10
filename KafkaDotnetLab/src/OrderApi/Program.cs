using Confluent.Kafka;
using Microsoft.Data.Sqlite;

namespace OrderApi;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.AddControllers();

        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        builder.Services.AddHostedService<OutboxDispatcher>();

        builder.Services.AddSingleton<IProducer<string, string>>(_ =>
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            return new ProducerBuilder<string, string>(config).Build();
        });

        var connectionString =
            builder.Configuration.GetConnectionString("OrderDatabase")
            ?? throw new InvalidOperationException(
                "OrderDatabase connection string is missing.");

        var app = builder.Build();

        if (app.Environment.IsDevelopment())
        {
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        app.UseHttpsRedirection();

        app.UseAuthorization();

        app.MapControllers();

        // DB initialization
        using (var connection = new SqliteConnection(connectionString))
        {
            connection.Open();

            var command = connection.CreateCommand();

            command.CommandText = """
                CREATE TABLE IF NOT EXISTS Orders (
                    Id TEXT PRIMARY KEY,
                    Product TEXT NOT NULL,
                    CreatedAtUtc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS OutboxMessages (
                    Id TEXT PRIMARY KEY,
                    EventType TEXT NOT NULL,
                    Payload TEXT NOT NULL,
                    CreatedAtUtc TEXT NOT NULL,
                    ProcessedAtUtc TEXT NULL
                );
                """;

            command.ExecuteNonQuery();
        }

        app.Run();
    }
}