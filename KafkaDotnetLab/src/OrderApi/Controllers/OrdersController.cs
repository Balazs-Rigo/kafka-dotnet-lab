using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace OrderApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class OrdersController : ControllerBase
{
    private readonly IProducer<string, string> _producer;

    public OrdersController(IProducer<string, string> producer)
    {
        _producer = producer;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder(OrderRequest request)
    {
        var orderId = Guid.NewGuid();

        var result = await _producer.ProduceAsync(
            "orders",
            new Message<string, string>
            {
                Key = orderId.ToString(),
                Value = request.Product
            });

        return Ok(new
        {
            OrderId = orderId,
            Topic = result.Topic,
            Partition = result.Partition.Value,
            Offset = result.Offset.Value
        });
    }
}

public record OrderRequest(string Product);