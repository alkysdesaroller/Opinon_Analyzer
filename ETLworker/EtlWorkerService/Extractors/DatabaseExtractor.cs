using EtlWorkerService.Models;
using Microsoft.Data.SqlClient;

namespace EtlWorkerService.Extractors;

public class DatabaseExtractor(IConfiguration configuration, ILogger<DatabaseExtractor> logger)
{
    private readonly ILogger<DatabaseExtractor> _logger = logger;
    private readonly string _connectionString = configuration.GetConnectionString("RelationalConnection")
                                                // ReSharper disable once NotResolvedInText
                                                ?? throw new ArgumentNullException("RelationalConnection no está configurado.");
    
    public async Task<IEnumerable<ComentarioApi>> ExtractComentariosAsync()
    {
        var lista = new List<ComentarioApi>();
 
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
 
            var command = new SqlCommand(@"
                SELECT 
                    c.Id,
                    c.IdComentario,
                    c.Comentario,
                    c.Fecha,
                    c.Rating,
                    c.PuntajeSatisfaccion,
                    c.Clasificacion,
                    c.Fuente,
                    cl.Nombre AS Cliente,
                    cl.Email,
                    p.NombreProducto,
                    p.Categoria
                FROM Comentarios c
                INNER JOIN Clients cl ON c.IdCliente = cl.IdCliente
                INNER JOIN Products p  ON c.IdProducto = p.IdProducto
            ", connection);
 
            using var reader = await command.ExecuteReaderAsync();
 
            while (await reader.ReadAsync())
            {
                lista.Add(new ComentarioApi
                {
                    Id                  = Convert.ToInt32(reader["Id"]),
                    IdComentario        = reader["IdComentario"].ToString()!,
                    Comentario          = reader["Comentario"] == DBNull.Value ? null : reader["Comentario"].ToString(),
                    Fecha               = Convert.ToDateTime(reader["Fecha"]),
                    Rating              = reader["Rating"] == DBNull.Value ? null : Convert.ToInt32(reader["Rating"]),
                    PuntajeSatisfaccion = reader["PuntajeSatisfaccion"] == DBNull.Value ? null : Convert.ToInt32(reader["PuntajeSatisfaccion"]),
                    Clasificacion       = reader["Clasificacion"] == DBNull.Value ? null : reader["Clasificacion"].ToString(),
                    Fuente              = reader["Fuente"].ToString()!,
                    Cliente             = reader["Cliente"] == DBNull.Value ? null : reader["Cliente"].ToString(),
                    Email               = reader["Email"] == DBNull.Value ? null : reader["Email"].ToString(),
                    NombreProducto      = reader["NombreProducto"] == DBNull.Value ? null : reader["NombreProducto"].ToString(),
                    Categoria           = reader["Categoria"] == DBNull.Value ? null : reader["Categoria"].ToString(),
                });
            }
 
            _logger.LogInformation("BD Relacional: {Count} comentarios extraídos", lista.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error extrayendo comentarios de la base de datos relacional");
        }
 
        return lista;
    }
}
