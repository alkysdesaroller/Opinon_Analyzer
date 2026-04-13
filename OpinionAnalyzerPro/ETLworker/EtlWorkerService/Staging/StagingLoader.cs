using EtlWorkerService.Models;
using Microsoft.Data.SqlClient;

namespace EtlWorkerService.Staging;

public class StagingLoader
{
     private readonly ILogger<StagingLoader> _logger;
    private readonly string _connectionString;
 
    public StagingLoader(IConfiguration configuration, ILogger<StagingLoader> logger)
    {
        _logger = logger;
        _connectionString = configuration.GetConnectionString("AnalyticConnection")
                            // ReSharper disable once NotResolvedInText
                            ?? throw new ArgumentNullException("AnalyticConnection no está configurado.");
    }
 
    // ─── Helpers ────────────────────────────────────────────────────────────────
 
    private async Task<SqlConnection> OpenConnectionAsync()
    {
        var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
 
    private async Task TruncateTableAsync(SqlConnection conn, string tableName)
    {
        using var cmd = new SqlCommand($"TRUNCATE TABLE {tableName}", conn);
        await cmd.ExecuteNonQueryAsync();
        _logger.LogInformation("Tabla {Table} limpiada antes de carga", tableName);
    }
 
    // ─── Clients ────────────────────────────────────────────────────────────────
 
    public async Task LoadClientsAsync(IEnumerable<ClientCsv> records)
    {
        using var conn = await OpenConnectionAsync();
        await TruncateTableAsync(conn, "Staging_Clients");
 
        int count = 0;
        foreach (var r in records)
        {
            using var cmd = new SqlCommand(@"
                INSERT INTO Staging_Clients (IdCliente, Nombre, Email)
                VALUES (@IdCliente, @Nombre, @Email)", conn);
 
            cmd.Parameters.AddWithValue("@IdCliente", r.IdCliente);
            cmd.Parameters.AddWithValue("@Nombre",    r.Nombre);
            cmd.Parameters.AddWithValue("@Email",     r.Email);
 
            await cmd.ExecuteNonQueryAsync();
            count++;
        }
        _logger.LogInformation("Staging_Clients: {Count} registros insertados", count);
    }
 
    // ─── Fuente de Datos ────────────────────────────────────────────────────────
 
    public async Task LoadFuenteDatosAsync(IEnumerable<FuenteDatosCsv> records)
    {
        using var conn = await OpenConnectionAsync();
        await TruncateTableAsync(conn, "Staging_FuenteDatos");
 
        int count = 0;
        foreach (var r in records)
        {
            using var cmd = new SqlCommand(@"
                INSERT INTO Staging_FuenteDatos (IdFuente, TipoFuente, FechaCarga)
                VALUES (@IdFuente, @TipoFuente, @FechaCarga)", conn);
 
            cmd.Parameters.AddWithValue("@IdFuente",   r.IdFuente);
            cmd.Parameters.AddWithValue("@TipoFuente", r.TipoFuente);
            cmd.Parameters.AddWithValue("@FechaCarga", r.FechaCarga);
 
            await cmd.ExecuteNonQueryAsync();
            count++;
        }
        _logger.LogInformation("Staging_FuenteDatos: {Count} registros insertados", count);
    }
 
    // ─── Products ───────────────────────────────────────────────────────────────
 
    public async Task LoadProductsAsync(IEnumerable<ProductCsv> records)
    {
        using var conn = await OpenConnectionAsync();
        await TruncateTableAsync(conn, "Staging_Products");
 
        int count = 0;
        foreach (var r in records)
        {
            using var cmd = new SqlCommand(@"
                INSERT INTO Staging_Products (IdProducto, Nombre, Categoria)
                VALUES (@IdProducto, @Nombre, @Categoria)", conn);
 
            cmd.Parameters.AddWithValue("@IdProducto", r.IdProducto);
            cmd.Parameters.AddWithValue("@Nombre",     r.Nombre);
            cmd.Parameters.AddWithValue("@Categoria",  r.Categoria);
 
            await cmd.ExecuteNonQueryAsync();
            count++;
        }
        _logger.LogInformation("Staging_Products: {Count} registros insertados", count);
    }
 
    // ─── Social Comments ────────────────────────────────────────────────────────
 
    public async Task LoadSocialCommentsAsync(IEnumerable<SocialCommentCsv> records)
    {
        using var conn = await OpenConnectionAsync();
        await TruncateTableAsync(conn, "Staging_SocialComments");
 
        int count = 0;
        foreach (var r in records)
        {
            using var cmd = new SqlCommand(@"
                INSERT INTO Staging_SocialComments (IdComment, IdCliente, IdProducto, Fuente, Fecha, Comentario)
                VALUES (@IdComment, @IdCliente, @IdProducto, @Fuente, @Fecha, @Comentario)", conn);
 
            cmd.Parameters.AddWithValue("@IdComment",  r.IdComment);
            cmd.Parameters.AddWithValue("@IdCliente",  r.IdCliente);
            cmd.Parameters.AddWithValue("@IdProducto", r.IdProducto);
            cmd.Parameters.AddWithValue("@Fuente",     r.Fuente);
            cmd.Parameters.AddWithValue("@Fecha",      r.Fecha);
            cmd.Parameters.AddWithValue("@Comentario", r.Comentario);
 
            await cmd.ExecuteNonQueryAsync();
            count++;
        }
        _logger.LogInformation("Staging_SocialComments: {Count} registros insertados", count);
    }
 
    // ─── Surveys Part 1 ─────────────────────────────────────────────────────────
 
    public async Task LoadSurveysAsync(IEnumerable<SurveyPart1Csv> records)
    {
        using var conn = await OpenConnectionAsync();
        await TruncateTableAsync(conn, "Staging_Surveys");
 
        int count = 0;
        foreach (var r in records)
        {
            using var cmd = new SqlCommand(@"
                INSERT INTO Staging_Surveys
                    (IdOpinion, IdCliente, IdProducto, Fecha, Comentario, Clasificacion, PuntajeSatisfaccion, Fuente)
                VALUES
                    (@IdOpinion, @IdCliente, @IdProducto, @Fecha, @Comentario, @Clasificacion, @PuntajeSatisfaccion, @Fuente)", conn);
 
            cmd.Parameters.AddWithValue("@IdOpinion",           r.IdOpinion);
            cmd.Parameters.AddWithValue("@IdCliente",           r.IdCliente);
            cmd.Parameters.AddWithValue("@IdProducto",          r.IdProducto);
            cmd.Parameters.AddWithValue("@Fecha",               r.Fecha);
            cmd.Parameters.AddWithValue("@Comentario",          r.Comentario);
            cmd.Parameters.AddWithValue("@Clasificacion",       r.Clasificacion);
            cmd.Parameters.AddWithValue("@PuntajeSatisfaccion", r.PuntajeSatisfaccion);
            cmd.Parameters.AddWithValue("@Fuente",              r.Fuente);
 
            await cmd.ExecuteNonQueryAsync();
            count++;
        }
        _logger.LogInformation("Staging_Surveys: {Count} registros insertados", count);
    }
 
    // ─── Web Reviews ────────────────────────────────────────────────────────────
 
    public async Task LoadWebReviewsAsync(IEnumerable<WebReviewCsv> records)
    {
        using var conn = await OpenConnectionAsync();
        await TruncateTableAsync(conn, "Staging_WebReviews");
 
        int count = 0;
        foreach (var r in records)
        {
            using var cmd = new SqlCommand(@"
                INSERT INTO Staging_WebReviews (IdReview, IdCliente, IdProducto, Fecha, Comentario, Rating)
                VALUES (@IdReview, @IdCliente, @IdProducto, @Fecha, @Comentario, @Rating)", conn);
 
            cmd.Parameters.AddWithValue("@IdReview",   r.IdReview);
            cmd.Parameters.AddWithValue("@IdCliente",  r.IdCliente);
            cmd.Parameters.AddWithValue("@IdProducto", r.IdProducto);
            cmd.Parameters.AddWithValue("@Fecha",      r.Fecha);
            cmd.Parameters.AddWithValue("@Comentario", r.Comentario);
            cmd.Parameters.AddWithValue("@Rating",     r.Rating);
 
            await cmd.ExecuteNonQueryAsync();
            count++;
        }
        _logger.LogInformation("Staging_WebReviews: {Count} registros insertados", count);
    }
 
    // ─── API / DB Comentarios ───────────────────────────────────────────────────
 
    public async Task LoadComentariosAsync(IEnumerable<ComentarioApi> records, string stagingTable)
    {
        using var conn = await OpenConnectionAsync();
        await TruncateTableAsync(conn, stagingTable);
 
        int count = 0;
        foreach (var r in records)
        {
            using var cmd = new SqlCommand($@"
                INSERT INTO {stagingTable}
                    (IdComentario, Comentario, Fecha, Rating, PuntajeSatisfaccion, Clasificacion, Fuente, Cliente, Email, NombreProducto, Categoria)
                VALUES
                    (@IdComentario, @Comentario, @Fecha, @Rating, @PuntajeSatisfaccion, @Clasificacion, @Fuente, @Cliente, @Email, @NombreProducto, @Categoria)", conn);
 
            cmd.Parameters.AddWithValue("@IdComentario",        r.IdComentario);
            cmd.Parameters.AddWithValue("@Comentario",          (object?)r.Comentario ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Fecha",               r.Fecha);
            cmd.Parameters.AddWithValue("@Rating",              (object?)r.Rating ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@PuntajeSatisfaccion", (object?)r.PuntajeSatisfaccion ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Clasificacion",       (object?)r.Clasificacion ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Fuente",              r.Fuente);
            cmd.Parameters.AddWithValue("@Cliente",             (object?)r.Cliente ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Email",               (object?)r.Email ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@NombreProducto",      (object?)r.NombreProducto ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Categoria",           (object?)r.Categoria ?? DBNull.Value);
 
            await cmd.ExecuteNonQueryAsync();
            count++;
        }
        _logger.LogInformation("{Table}: {Count} registros insertados", stagingTable, count);
    }
}