using Confluent.Kafka;
using Producer;
using System.Diagnostics.Metrics;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast = Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast")
.WithOpenApi();

app.MapPost("/sendkafka", () =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = "host.docker.internal:9092",
        ClientId = "dotnet-console",
        TransactionalId = "Producer01"
    };

    var topic = "WebApiKafka";

    using (var producer = new ProducerBuilder<string, string>(config).Build())
    {
        producer.InitTransactions(TimeSpan.FromSeconds(10)); // start transaction in 10 second
        string? key = "";

        List<Product> products = new List<Product>();

        // Menambahkan objek Product pertama
        Product product1 = new Product
        {
            Id = 1,
            Name = "Buku",
            Price = 7.5f,
            Stock = 45
        };
        products.Add(product1);

        // Menambahkan objek Product kedua
        Product product2 = new Product
        {
            Id = 2,
            Name = "Pensil",
            Price = 1.2f,
            Stock = 100
        };
        products.Add(product2);

        // Menambahkan objek Product ketiga
        Product product3 = new Product
        {
            Id = 3,
            Name = "Pulpen",
            Price = 2.5f,
            Stock = 75
        };
        products.Add(product3);

        var value = Newtonsoft.Json.JsonConvert.SerializeObject(products);

        // mulai transaction
        producer.BeginTransaction();
        producer.Produce(topic, new Message<string, string>
        {
            Key = key,
            Value = value
        }, (deliveryReport) =>
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

        return products;
    }


});
app.Run();

internal record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
