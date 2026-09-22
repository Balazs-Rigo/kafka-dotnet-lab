using Confluent.Kafka;

namespace RetryConsumerDemo;

internal class Program
{
    static async Task Main(string[] args)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "retry-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using var consumer =
            new ConsumerBuilder<string, string>(consumerConfig).Build();

        using var producer =
            new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe("retry-demo-retry");

        Console.WriteLine("Retry consumer waiting for messages...");

        while (true)
        {
            var result = consumer.Consume();

            Console.WriteLine();
            Console.WriteLine($"Retry message received: {result.Message.Value}");
            Console.WriteLine($"Offset: {result.Offset}");

            const int maxRetries = 3;
            var success = false;

            for (var attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    Console.WriteLine($"Retry attempt {attempt}/{maxRetries}");

                    ProcessMessage(result.Message.Value);

                    success = true;
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Retry failed: {ex.Message}");

                    if (attempt < maxRetries)
                    {
                        Console.WriteLine("Waiting 1 second...");
                        await Task.Delay(1000);
                    }
                }
            }

            if (!success)
            {
                Console.WriteLine("All retry attempts failed.");
                Console.WriteLine("Sending message to DLQ...");

                await producer.ProduceAsync(
                    "retry-demo-dlq",
                    new Message<string, string>
                    {
                        Key = result.Message.Key,
                        Value = result.Message.Value
                    });

                Console.WriteLine("Message sent to DLQ.");
            }
            else
            {
                Console.WriteLine("Retry processing succeeded.");
            }

            consumer.Commit(result);

            Console.WriteLine("Retry topic offset committed.");
        }
    }

    private static void ProcessMessage(string value)
    {
        if (value == "order-bad")
        {
            throw new Exception("Simulated permanent payment failure");
        }

        Console.WriteLine($"Successfully processed on retry: {value}");
    }
}