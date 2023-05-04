// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Newtonsoft.Json;
using Producer;

Console.WriteLine("Produces app");

var config = new ProducerConfig
{
    BootstrapServers = "127.0.0.1:9092",
    ClientId = "dotnet-console",
    TransactionalId = "Producer01"
};

var topic = "demo123";

using (var producer = new ProducerBuilder<string, string>(config).Build())
{
    Console.WriteLine("kafka connected");
    producer.InitTransactions(TimeSpan.FromSeconds(10)); // start transaction in 10 second
    var counter = 1;
    while (true)
    {
        var key = "key" + counter;
        // var value = "message" + counter.ToString();
        var product = new Product
        {
            Id = counter,
            Name = $"Product {counter.ToString()}",
            Stock = counter,
            Price = counter * 1.2f
        };
        var value = Newtonsoft.Json.JsonConvert.SerializeObject(product);
        // mulai transaction
        producer.BeginTransaction();
        producer.Produce(topic, new Message<string, string>
        {
            Key = key,
            Value = value
        },(deliveryReport) =>
        {
            if (deliveryReport.Error.Code != ErrorCode.NoError)
            {
                Console.WriteLine("Failed to send message");
            }
            else
            {
                Console.WriteLine($"Succeed. PartitionOfMessage : {deliveryReport.TopicPartitionOffset}");
            }
        });
        producer.Flush(TimeSpan.FromSeconds(5)); // membersihkan buffer(tempat menyimpan data sementara)
        // commit transaction
        producer.CommitTransaction();
        counter++;
        await Task.Delay(TimeSpan.FromSeconds(5));
    }
}
