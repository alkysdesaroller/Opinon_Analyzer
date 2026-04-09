using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpinionesETL.Configuration;
using OpinionesETL.Extractors;
using OpinionesETL.Loaders;
using OpinionesETL.Models;

namespace OpinionesETL.Workers;

/// <summary>
/// Worker principal del proceso ETL.
/// Orquesta la extracción paralela de las tres fuentes y la carga en la BD Analítica.
/// Escalabilidad: cada extractor es intercambiable gracias a IExtractor.
/// </summary>
public class EtlWorker : BackgroundService
{
    private readonly ILogger<EtlWorker> _logger;
    private readonly IEnumerable<IExtractor> _extractors;
    private readonly DataLoader _loader;
    private readonly ExtractorSettings _settings;

    public EtlWorker(ILogger<EtlWorker> logger,
                     IEnumerable<IExtractor> extractors,
                     DataLoader loader,
                     IOptions<ExtractorSettings> settings)
    {
        _logger     = logger;
        _extractors = extractors;
        _loader     = loader;
        _settings   = settings.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("[EtlWorker] Worker iniciado. Intervalo: {Min} min.", _settings.IntervalMinutes);

        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("[EtlWorker] Iniciando ciclo ETL. {Time}", DateTimeOffset.Now);

            try
            {
                // Extracción paralela de todas las fuentes (Task.WhenAll)
                var extractionTasks = _extractors
                    .Select(e => e.ExtractAsync(stoppingToken))
                    .ToArray();

                var allResults = await Task.WhenAll(extractionTasks);

                // Aplanar todas las colecciones en una sola lista
                var allOpinions = allResults
                    .SelectMany(r => r)
                    .ToList();

                _logger.LogInformation("[EtlWorker] Total extraído de todas las fuentes: {Count} registros.",
                    allOpinions.Count);

                // Cargar en BD Analítica
                await _loader.LoadAsync(allOpinions, stoppingToken);

                _logger.LogInformation("[EtlWorker] Ciclo ETL completado exitosamente.");
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("[EtlWorker] Ciclo cancelado por señal de parada.");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[EtlWorker] Error no controlado en el ciclo ETL. Reintentando en el próximo ciclo.");
            }

            // Espera hasta el próximo ciclo
            await Task.Delay(TimeSpan.FromMinutes(_settings.IntervalMinutes), stoppingToken);
        }

        _logger.LogInformation("[EtlWorker] Worker detenido.");
    }
}
