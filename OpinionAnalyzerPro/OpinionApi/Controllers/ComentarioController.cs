using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using OpinionApi.Dto;

namespace OpinionApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ComentarioController(IConfiguration configuration) : ControllerBase
{
    private readonly IConfiguration _configuration = configuration;

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        var lista = new List<ComentarioResponseDto>();

        using var connection = new SqlConnection(connectionString);
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
            INNER JOIN Products p ON c.IdProducto = p.IdProducto
        ", connection);

        using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            lista.Add(new ComentarioResponseDto
            {
                Id                  = Convert.ToInt32(reader["Id"]),
                IdComentario        = reader["IdComentario"].ToString()!,
                Comentario          = reader["Comentario"] == DBNull.Value
                                        ? null
                                        : reader["Comentario"].ToString(),
                Fecha               = Convert.ToDateTime(reader["Fecha"]),
                Rating              = reader["Rating"] == DBNull.Value
                                        ? null
                                        : Convert.ToInt32(reader["Rating"]),
                PuntajeSatisfaccion = reader["PuntajeSatisfaccion"] == DBNull.Value
                                        ? null
                                        : Convert.ToInt32(reader["PuntajeSatisfaccion"]),
                Clasificacion       = reader["Clasificacion"] == DBNull.Value
                                        ? null
                                        : reader["Clasificacion"].ToString(),
                Fuente              = reader["Fuente"].ToString()!,
                Cliente             = reader["Cliente"] == DBNull.Value
                                        ? null
                                        : reader["Cliente"].ToString(),
                Email               = reader["Email"] == DBNull.Value
                                        ? null
                                        : reader["Email"].ToString(),
                NombreProducto      = reader["NombreProducto"] == DBNull.Value
                                        ? null
                                        : reader["NombreProducto"].ToString(),
                Categoria           = reader["Categoria"] == DBNull.Value
                                        ? null
                                        : reader["Categoria"].ToString(),
            });
        }

        return Ok(lista);
    }
}