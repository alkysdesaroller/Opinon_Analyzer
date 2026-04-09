using Microsoft.Extensions.Options;
using OpinionesETL.Configuration;
using OpinionesETL.Extractors;
using OpinionesETL.Loaders;
using OpinionesETL.Workers;

var builder = Host.CreateApplicationBuilder(args);

// ── Configuración ──────────────────────────────────────────────────────────
builder.Services.Configure<ExtractorSettings>(
    builder.Configuration.GetSection("ExtractorSettings"));

// ── Extractores (registrados como IExtractor) ──────────────────────────────
// Escalabilidad: agregar nuevas fuentes es solo registrar una nueva clase aquí.
builder.Services.AddTransient<IExtractor, CsvExtractor>();
builder.Services.AddTransient<IExtractor, DatabaseExtractor>();
builder.Services.AddTransient<IExtractor, ApiExtractor>();

// ── HttpClient para la API REST (con BaseAddress desde config) ─────────────
builder.Services.AddHttpClient("OpinionesApi", (sp, client) =>
{
    var settings = sp.GetRequiredService<IOptions<ExtractorSettings>>().Value;
    client.BaseAddress = new Uri(settings.ApiBaseUrl);
    client.Timeout = TimeSpan.FromSeconds(30);
});

// ── DataLoader y Worker ────────────────────────────────────────────────────
builder.Services.AddTransient<DataLoader>();
builder.Services.AddHostedService<EtlWorker>();

// ── Logging estructurado (puede extenderse con Serilog) ────────────────────
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

var host = builder.Build();
await host.RunAsync();
