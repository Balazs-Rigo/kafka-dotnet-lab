using Confluent.Kafka;

namespace ConsumerDemo;

internal class Program
{
    static async Task Main(string[] args)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "retry-demo-group",
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

        consumer.Subscribe("retry-demo");

        Console.WriteLine("Waiting for messages...");

        while (true)
        {
            var result = consumer.Consume();

            Console.WriteLine();
            Console.WriteLine($"Received: {result.Message.Value}");
            Console.WriteLine($"Offset: {result.Offset}");

            try
            {
                ProcessMessage(result.Message.Value);

                consumer.Commit(result);

                Console.WriteLine("Successfully processed.");
                Console.WriteLine("Original offset committed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Processing failed: {ex.Message}");
                Console.WriteLine("Sending to retry topic...");

                await producer.ProduceAsync(
                    "retry-demo-retry",
                    new Message<string, string>
                    {
                        Key = result.Message.Key,
                        Value = result.Message.Value
                    });

                Console.WriteLine("Message moved to retry topic.");

                consumer.Commit(result);

                Console.WriteLine("Original offset committed.");
            }
        }
    }

    private static void ProcessMessage(string value)
    {
        if (value == "order-bad")
        {
            throw new Exception("Simulated payment failure");
        }

        Console.WriteLine($"Successfully processed: {value}");
    }
}