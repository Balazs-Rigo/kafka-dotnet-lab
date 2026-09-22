using Confluent.Kafka;
using OrderProcessor;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IConsumer<string, string>>(_ =>
{
    var config = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "order-processor-group",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false
    };

    return new ConsumerBuilder<string, string>(config).Build();
});

builder.Services.AddHostedService<Worker>();

var host = builder.Build();

host.Run();