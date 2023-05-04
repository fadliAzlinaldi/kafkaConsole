// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Producer;

Console.WriteLine("Consumer app");

var config = new ConsumerConfig
{
    BootstrapServers = "127.0.0.1:9092",
    GroupId = "tester",
    AutoOffsetReset = AutoOffsetReset.Earliest // from beginnig
};

var topic = "WebApiKafka";
// break console
CancellationTokenSource cts = new CancellationTokenSource();

// lamda or anonymous fungcition
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};


// method biasa

//Console.CancelKeyPress += Console_CancelKeyPress;

//void Console_CancelKeyPress(object? sender, ConsoleCancelEventArgs e)
//{
//    e.Cancel = true;
//    cts.Cancel();
//}

// [] --> array
// <> --> dictionary
// () tuple --> pyhton
using (var consumer = new ConsumerBuilder<string, string>(config).Build())
{
    Console.WriteLine("Kafka Connected");
    consumer.Subscribe(topic);
    try
    {
        while (true)
        {
            var cr = consumer.Consume(cts.Token); // blocking
            Console.WriteLine($"Key : {cr.Message.Key} value : {cr.Message.Value}");
            if (cr != null)
            {
                var product = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Product>>(cr.Message.Value);
                // Console.WriteLine($">>>> {product?.Name}");
            }
        }
    }
    catch (OperationCanceledException)
    {
        // CTRL + C 
        Console.WriteLine("CTRL + C was pressed");
    }
    finally
    {
        consumer.Close();
    }
}