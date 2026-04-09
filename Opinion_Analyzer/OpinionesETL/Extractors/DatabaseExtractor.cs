using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpinionesETL.Configuration;
using OpinionesETL.Models;

namespace OpinionesETL.Extractors;

/// <summary>
/// Extrae reseñas desde SQL Server usando ADO.NET.
/// Seguridad: la cadena de conexión se lee de appsettings.json (variable de entorno en producción).
/// Rendimiento: consulta paginada con parámetros para evitar SQL Injection y full scans.
/// </summary>
public class DatabaseExtractor : IExtractor
{
    private readonly ILogger<DatabaseExtractor> _logger;
    private readonly ExtractorSettings _settings;

    public DatabaseExtractor(ILogger<DatabaseExtractor> logger, IOptions<ExtractorSettings> settings)
    {
        _logger = logger;
        _settings = settings.Value;
    }

    public async Task<IEnumerable<OpinionRaw>> ExtractAsync(CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        _logger.LogInformation("[DatabaseExtractor] Iniciando extracción desde SQL Server.");

        var results = new List<OpinionRaw>();

        try
        {
            await using var conn = new SqlConnection(_settings.DatabaseConnectionString);
            await conn.OpenAsync(cancellationToken);

            // Consulta parametrizada: trae solo reseñas no procesadas
            var query = @"
                SELECT r.ReviewId, r.ProductId, r.CustomerId, r.Rating,
                       r.CommentText, r.ReviewDate,
                       c.Email, c.CustomerName, c.Country
                FROM Reviews r
                INNER JOIN Customers c ON r.CustomerId = c.CustomerId
                WHERE r.IsProcessed = 0
                ORDER BY r.ReviewDate ASC";

            await using var cmd = new SqlCommand(query, conn);
            cmd.CommandTimeout = 60;

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                results.Add(new OpinionRaw
                {
                    SourceType    = "review",
                    ProductId     = reader["ProductId"]?.ToString(),
                    CustomerId    = reader["CustomerId"]?.ToString(),
                    CustomerEmail = reader["Email"]?.ToString(),
                    CustomerName  = reader["CustomerName"]?.ToString(),
                    Country       = reader["Country"]?.ToString(),
                    Rating        = reader["Rating"] is DBNull ? null : Convert.ToInt32(reader["Rating"]),
                    CommentText   = reader["CommentText"]?.ToString(),
                    OpinionDate   = reader["ReviewDate"] is DBNull ? null : Convert.ToDateTime(reader["ReviewDate"])
                });
            }

            sw.Stop();
            _logger.LogInformation("[DatabaseExtractor] Extracción completa. Registros: {Count}. Tiempo: {Ms}ms",
                results.Count, sw.ElapsedMilliseconds);
        }
        catch (SqlException ex)
        {
            _logger.LogError(ex, "[DatabaseExtractor] Error de SQL Server. Número: {Number}", ex.Number);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "[DatabaseExtractor] Error inesperado durante la extracción");
        }

        return results;
    }
}
