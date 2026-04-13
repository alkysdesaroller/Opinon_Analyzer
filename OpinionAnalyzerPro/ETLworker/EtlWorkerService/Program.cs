using EtlWorkerService;
using EtlWorkerService.Configuration;
using EtlWorkerService.Extractors;
using EtlWorkerService.Staging;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHttpClient("OpinionApi", client =>
{
    client.Timeout = TimeSpan.FromSeconds(30);
}).ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
{
    ServerCertificateCustomValidationCallback = 
        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
});

builder.Services.Configure<EtlSettings>(
    builder.Configuration.GetSection("Etl"));

builder.Services.AddSingleton<CsvExtractor>();
builder.Services.AddSingleton<DatabaseExtractor>();
builder.Services.AddSingleton<ApiExtractor>();
builder.Services.AddSingleton<StagingLoader>();

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();