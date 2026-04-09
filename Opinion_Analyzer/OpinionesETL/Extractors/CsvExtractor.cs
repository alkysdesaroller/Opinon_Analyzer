using System.Diagnostics;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpinionesETL.Configuration;
using OpinionesETL.Models;

namespace OpinionesETL.Extractors;

/// <summary>
/// Extrae opiniones desde archivos CSV de encuestas internas.
/// Rendimiento: lectura en streaming con CsvHelper (sin cargar todo en memoria).
/// </summary>
public class CsvExtractor : IExtractor
{
    private readonly ILogger<CsvExtractor> _logger;
    private readonly ExtractorSettings _settings;

    public CsvExtractor(ILogger<CsvExtractor> logger, IOptions<ExtractorSettings> settings)
    {
        _logger = logger;
        _settings = settings.Value;
    }

    public async Task<IEnumerable<OpinionRaw>> ExtractAsync(CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        _logger.LogInformation("[CsvExtractor] Iniciando extracción. Ruta: {Path}", _settings.CsvFilePath);

        var results = new List<OpinionRaw>();

        try
        {
            if (!File.Exists(_settings.CsvFilePath))
                throw new FileNotFoundException($"Archivo CSV no encontrado: {_settings.CsvFilePath}");

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                MissingFieldFound = null,
                BadDataFound = ctx =>
                    _logger.LogWarning("[CsvExtractor] Dato inválido en fila {Row}: {Field}",
                        ctx.Context.Parser.Row, ctx.Field)
            };

            using var reader = new StreamReader(_settings.CsvFilePath);
            using var csv = new CsvReader(reader, config);

            await foreach (var record in csv.GetRecordsAsync<CsvSurveyRecord>(cancellationToken))
            {
                results.Add(new OpinionRaw
                {
                    SourceType    = "encuesta",
                    ProductId     = record.ProductId,
                    CustomerId    = record.CustomerId,
                    CustomerEmail = record.Email,
                    CustomerName  = record.CustomerName,
                    Country       = record.Country,
                    Rating        = record.Score,
                    CommentText   = record.Comment,
                    OpinionDate   = record.Date,
                    Classification = record.Classification,
                    Score         = record.Score
                });
            }

            sw.Stop();
            _logger.LogInformation("[CsvExtractor] Extracción completa. Registros: {Count}. Tiempo: {Ms}ms",
                results.Count, sw.ElapsedMilliseconds);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "[CsvExtractor] Error durante la extracción");
        }

        return results;
    }
}

/// <summary>
/// Mapeo directo de columnas del CSV de encuestas.
/// </summary>
public class CsvSurveyRecord
{
    public string? ProductId { get; set; }
    public string? CustomerId { get; set; }
    public string? Email { get; set; }
    public string? CustomerName { get; set; }
    public string? Country { get; set; }
    public int? Score { get; set; }
    public string? Classification { get; set; }
    public string? Comment { get; set; }
    public DateTime? Date { get; set; }
}
