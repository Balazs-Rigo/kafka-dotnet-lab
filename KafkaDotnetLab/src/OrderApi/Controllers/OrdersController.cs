using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using OrderApi.Events;
using System.Text.Json;

namespace OrderApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
    private readonly string _connectionString;

    public OrdersController(IConfiguration configuration)
    {
        _connectionString =
            configuration.GetConnectionString("OrderDatabase")
            ?? throw new InvalidOperationException(
                "OrderDatabase connection string is missing.");
    }

    [HttpPost]
    public IActionResult CreateOrder(OrderRequest request)
    {
        var orderId = Guid.NewGuid();
        var createdAtUtc = DateTime.UtcNow;

        var orderCreatedEvent = new OrderCreatedEvent(
            2,
            orderId,
            request.Product.ToString(),
            createdAtUtc,
            1999.99m);

        var payload = JsonSerializer.Serialize(orderCreatedEvent);

        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        using var transaction = connection.BeginTransaction();

        try
        {
            using (var orderCommand = connection.CreateCommand())
            {
                orderCommand.Transaction = transaction;

                orderCommand.CommandText = """
                    INSERT INTO Orders (
                        Id,
                        Product,
                        CreatedAtUtc
                    )
                    VALUES (
                        $id,
                        $product,
                        $createdAtUtc
                    );
                    """;

                orderCommand.Parameters.AddWithValue(
                    "$id",
                    orderId.ToString());

                orderCommand.Parameters.AddWithValue(
                    "$product",
                    request.Product.ToString());

                orderCommand.Parameters.AddWithValue(
                    "$createdAtUtc",
                    createdAtUtc.ToString("O"));

                orderCommand.ExecuteNonQuery();
            }

            using (var outboxCommand = connection.CreateCommand())
            {
                outboxCommand.Transaction = transaction;

                outboxCommand.CommandText = """
                    
                        INSERT INTO OutboxMessages (
                        Id,
                        EventType,
                        Payload,
                        CreatedAtUtc,
                        ProcessedAtUtc
                        )
                        VALUES (
                        $id,
                        $eventType,
                        $payload,
                        $createdAtUtc,
                        NULL
                        );
                    
                    """;

                outboxCommand.Parameters.AddWithValue(
                    "$id",
                    Guid.NewGuid().ToString());

                outboxCommand.Parameters.AddWithValue(
                    "$eventType",
                    "OrderCreated");

                outboxCommand.Parameters.AddWithValue(
                    "$payload",
                    payload);

                outboxCommand.Parameters.AddWithValue(
                    "$createdAtUtc",
                    createdAtUtc.ToString("O"));

                outboxCommand.ExecuteNonQuery();
            }

            transaction.Commit();

            return Ok(new
            {
                OrderId = orderId,
                SavedToDatabase = true,
                OutboxCreated = true
            });
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }
}

public record OrderRequest(string Product);