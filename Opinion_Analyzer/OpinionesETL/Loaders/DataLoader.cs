using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpinionesETL.Configuration;
using OpinionesETL.Models;

namespace OpinionesETL.Loaders;

/// <summary>
/// Inserta los datos extraídos en la base de datos analítica (modelo estrella).
/// Estrategia: primero staging temporal, luego MERGE a tablas Dim/Fact.
/// Rendimiento: SqlBulkCopy para staging (mucho más rápido que INSERT individual).
/// </summary>
public class DataLoader
{
    private readonly ILogger<DataLoader> _logger;
    private readonly ExtractorSettings _settings;

    public DataLoader(ILogger<DataLoader> logger, IOptions<ExtractorSettings> settings)
    {
        _logger = logger;
        _settings = settings.Value;
    }

    public async Task LoadAsync(IEnumerable<OpinionRaw> opinions, CancellationToken cancellationToken = default)
    {
        var list = opinions.ToList();
        if (list.Count == 0)
        {
            _logger.LogWarning("[DataLoader] No hay datos para cargar.");
            return;
        }

        _logger.LogInformation("[DataLoader] Cargando {Count} registros en Staging...", list.Count);

        try
        {
            await using var conn = new SqlConnection(_settings.AnalyticConnectionString);
            await conn.OpenAsync(cancellationToken);

            // 1. Insertar en tabla staging
            await BulkInsertStagingAsync(conn, list, cancellationToken);

            // 2. MERGE de staging a tablas Dim/Fact
            await MergeToAnalyticAsync(conn, cancellationToken);

            _logger.LogInformation("[DataLoader] Carga en BD Analítica completada.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "[DataLoader] Error durante la carga de datos");
            throw;
        }
    }

    private static async Task BulkInsertStagingAsync(SqlConnection conn,
        List<OpinionRaw> data, CancellationToken ct)
    {
        // Construir DataTable para SqlBulkCopy
        var table = new DataTable("Staging_Opinions");
        table.Columns.Add("SourceType",     typeof(string));
        table.Columns.Add("ProductId",      typeof(string));
        table.Columns.Add("CustomerId",     typeof(string));
        table.Columns.Add("CustomerEmail",  typeof(string));
        table.Columns.Add("CustomerName",   typeof(string));
        table.Columns.Add("Country",        typeof(string));
        table.Columns.Add("Rating",         typeof(int));
        table.Columns.Add("CommentText",    typeof(string));
        table.Columns.Add("OpinionDate",    typeof(DateTime));
        table.Columns.Add("Classification", typeof(string));
        table.Columns.Add("Score",          typeof(int));
        table.Columns.Add("Platform",       typeof(string));
        table.Columns.Add("Sentiment",      typeof(string));

        foreach (var o in data)
        {
            table.Rows.Add(
                o.SourceType, o.ProductId, o.CustomerId, o.CustomerEmail,
                o.CustomerName, o.Country,
                (object?)o.Rating      ?? DBNull.Value,
                (object?)o.CommentText ?? DBNull.Value,
                (object?)o.OpinionDate ?? DBNull.Value,
                (object?)o.Classification ?? DBNull.Value,
                (object?)o.Score       ?? DBNull.Value,
                (object?)o.Platform    ?? DBNull.Value,
                (object?)o.Sentiment   ?? DBNull.Value
            );
        }

        using var bulk = new SqlBulkCopy(conn)
        {
            DestinationTableName = "Staging_Opinions",
            BatchSize = 500,
            BulkCopyTimeout = 120
        };

        await bulk.WriteToServerAsync(table, ct);
    }

    private static async Task MergeToAnalyticAsync(SqlConnection conn, CancellationToken ct)
    {
        // SP encapsula la lógica de MERGE para dimensiones y tabla de hechos
        await using var cmd = new SqlCommand("usp_MergeStagingToAnalytic", conn)
        {
            CommandType    = CommandType.StoredProcedure,
            CommandTimeout = 180
        };
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
