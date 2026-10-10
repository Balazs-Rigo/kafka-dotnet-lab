using Confluent.Kafka;

namespace OrderProcessor;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        builder.Services
            .AddSingleton<IConsumer<string, string>>(_ =>
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = "localhost:9092",
                    GroupId = "order-processor-group",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false
                };
                return new ConsumerBuilder<string, string>(config).Build();
            })
            .AddSingleton<IProducer<string, string>>(_ =>
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = "localhost:9092"
                };
                return new ProducerBuilder<string, string>(config).Build();
            });

        builder.Services.AddHostedService<Worker>();

        var host = builder.Build();

        host.Run();
    }
}