using Confluent.Kafka;

namespace ProducerDemo;

internal class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using var producer =
            new ProducerBuilder<string, string>(config).Build();

        var keys = new[]
        {
            "key-1",
            "key-2",
            "key-3",
            "key-4",
            "key-5"
        };

        foreach (var key in keys)
        {
            var result = await producer.ProduceAsync(
                "first_topic",
                new Message<string, string>
                {
                    Key = key,
                    Value = $"Message for {key}"
                });

            Console.WriteLine(
                $"{key} -> partition {result.Partition}, offset {result.Offset}");
        }
    }
}