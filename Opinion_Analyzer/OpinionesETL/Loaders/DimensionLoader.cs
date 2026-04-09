using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpinionesETL.Configuration;

namespace OpinionesETL.Loaders;

/// <summary>
/// Carga inicial de todas las dimensiones del DataWarehouse.
/// Ejecutar una sola vez antes del proceso ETL principal.
/// </summary>
public class DimensionLoader
{
    private readonly ILogger<DimensionLoader> _logger;
    private readonly string _connStr;

    public DimensionLoader(ILogger<DimensionLoader> logger, IOptions<ExtractorSettings> settings)
    {
        _logger  = logger;
        _connStr = settings.Value.AnalyticConnectionString;
    }

    public async Task LoadAllAsync(CancellationToken ct = default)
    {
        _logger.LogInformation("[DimensionLoader] Iniciando carga de dimensiones...");
        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync(ct);

        await LoadDimSentimentAsync(conn, ct);
        await LoadDimSourceAsync(conn, ct);
        await LoadDimDateAsync(conn, ct);
        await LoadDimProductAsync(conn, ct);
        await LoadDimCustomerAsync(conn, ct);

        _logger.LogInformation("[DimensionLoader] Todas las dimensiones cargadas.");
    }

    // ── DimSentiment ──────────────────────────────────────────────────────────
    private async Task LoadDimSentimentAsync(SqlConnection conn, CancellationToken ct)
    {
        _logger.LogInformation("[DimSentiment] Cargando...");
        var sql = @"
            IF NOT EXISTS (SELECT 1 FROM DimSentiment WHERE SentimentLabel = 'Positivo')
                INSERT INTO DimSentiment (SentimentLabel) VALUES ('Positivo');
            IF NOT EXISTS (SELECT 1 FROM DimSentiment WHERE SentimentLabel = 'Negativo')
                INSERT INTO DimSentiment (SentimentLabel) VALUES ('Negativo');
            IF NOT EXISTS (SELECT 1 FROM DimSentiment WHERE SentimentLabel = 'Neutro')
                INSERT INTO DimSentiment (SentimentLabel) VALUES ('Neutro');
            IF NOT EXISTS (SELECT 1 FROM DimSentiment WHERE SentimentLabel = 'Sin clasificar')
                INSERT INTO DimSentiment (SentimentLabel) VALUES ('Sin clasificar');";
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 60 };
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("[DimSentiment] OK — 4 registros precargados.");
    }

    // ── DimSource ─────────────────────────────────────────────────────────────
    private async Task LoadDimSourceAsync(SqlConnection conn, CancellationToken ct)
    {
        _logger.LogInformation("[DimSource] Cargando...");
        var sources = new[]
        {
            ("Encuestas Internas", "encuesta"),
            ("Reseñas Web",        "web"),
            ("Redes Sociales",     "social")
        };
        foreach (var (name, type) in sources)
        {
            var sql = @"
                IF NOT EXISTS (SELECT 1 FROM DimSource WHERE SourceName = @name)
                    INSERT INTO DimSource (SourceName, SourceType, LoadDate)
                    VALUES (@name, @type, GETDATE());";
            await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 60 };
            cmd.Parameters.AddWithValue("@name", name);
            cmd.Parameters.AddWithValue("@type", type);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        _logger.LogInformation("[DimSource] OK — 3 fuentes registradas.");
    }

    // ── DimDate ───────────────────────────────────────────────────────────────
    private async Task LoadDimDateAsync(SqlConnection conn, CancellationToken ct)
    {
        _logger.LogInformation("[DimDate] Cargando rango 2024-01-01 a 2026-12-31...");
        var sql = @"
            DECLARE @start DATE = '2024-01-01';
            DECLARE @end   DATE = '2026-12-31';
            DECLARE @cur   DATE = @start;
            WHILE @cur <= @end
            BEGIN
                DECLARE @key INT = CONVERT(INT, FORMAT(@cur, 'yyyyMMdd'));
                IF NOT EXISTS (SELECT 1 FROM DimDate WHERE DateKey = @key)
                    INSERT INTO DimDate (DateKey, FullDate, Year, Quarter, Month, MonthName, Week, Day)
                    VALUES (
                        @key,
                        @cur,
                        YEAR(@cur),
                        DATEPART(QUARTER, @cur),
                        MONTH(@cur),
                        DATENAME(MONTH, @cur),
                        DATEPART(WEEK, @cur),
                        DAY(@cur)
                    );
                SET @cur = DATEADD(DAY, 1, @cur);
            END";
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 300 };
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("[DimDate] OK — fechas 2024–2026 cargadas.");
    }

    // ── DimProduct ────────────────────────────────────────────────────────────
    private async Task LoadDimProductAsync(SqlConnection conn, CancellationToken ct)
    {
        _logger.LogInformation("[DimProduct] Cargando desde CSV de encuestas...");
        var sql = @"
            MERGE DimProduct AS tgt
            USING (
                SELECT DISTINCT
                    s.ProductId,
                    COALESCE(s.ProductName, 'Producto ' + s.ProductId) AS ProductName,
                    COALESCE(s.Category,    'General')                 AS Category,
                    COALESCE(s.Price,       0)                         AS Price
                FROM Staging_Opinions s
                WHERE s.ProductId IS NOT NULL
            ) AS src ON tgt.ProductId = src.ProductId
            WHEN NOT MATCHED THEN
                INSERT (ProductId, ProductName, Category, Price)
                VALUES (src.ProductId, src.ProductName, src.Category, src.Price);";
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
        var rows = await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("[DimProduct] OK — {Rows} productos insertados.", rows);
    }

    // ── DimCustomer ───────────────────────────────────────────────────────────
    private async Task LoadDimCustomerAsync(SqlConnection conn, CancellationToken ct)
    {
        _logger.LogInformation("[DimCustomer] Cargando clientes únicos de Staging...");
        var sql = @"
            MERGE DimCustomer AS tgt
            USING (
                SELECT DISTINCT
                    s.CustomerId,
                    COALESCE(s.CustomerEmail, 'sin-email@desconocido.com') AS Email,
                    COALESCE(s.CustomerName,  'Desconocido')               AS CustomerName,
                    COALESCE(s.Country,       'N/A')                       AS Country
                FROM Staging_Opinions s
                WHERE s.CustomerId IS NOT NULL
            ) AS src ON tgt.CustomerId = src.CustomerId
            WHEN NOT MATCHED THEN
                INSERT (CustomerId, Email, CustomerName, Country)
                VALUES (src.CustomerId, src.Email, src.CustomerName, src.Country);";
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
        var rows = await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogInformation("[DimCustomer] OK — {Rows} clientes insertados.", rows);
    }
}
