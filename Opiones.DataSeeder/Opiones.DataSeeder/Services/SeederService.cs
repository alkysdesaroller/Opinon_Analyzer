
using System.Globalization;
using System.Text;
using CsvHelper;
using Microsoft.Data.SqlClient;

namespace Opiones.DataSeeder.Services;

public class SeederService
{
    private readonly string _connectionString;

    public SeederService(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task Execute()
    {
        var comentarios = new List<ComentarioCsv>();
        comentarios.AddRange(LeerWebReviews("data/web_reviews.csv"));
        comentarios.AddRange(LeerSocial("Data/social_comments.csv"));
        comentarios.AddRange(LeerSurveys("Data/surveys_part1.csv"));

        comentarios = LimpiarComentarios(comentarios);

        var productos = LeerProductos("data/products.csv");

        await InsertarEnBaseDeDatos(comentarios, productos);

        Console.WriteLine($"✔ {comentarios.Count} comentarios insertados correctamente.");
    }

    private List<ComentarioCsv> LeerWebReviews(string path)
    {
        using var reader = new StreamReader(path, Encoding.UTF8);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

        var records = csv.GetRecords<dynamic>().ToList();

        return records.Select(r => new ComentarioCsv
        {
            IdComentario = r.IdReview,
            IdClients = r.IdCliente,
            IdProducto = r.IdProducto,
            Fecha = Convert.ToDateTime(r.Fecha),
            Comentario = r.Comentario,
            Rating = string.IsNullOrEmpty(r.Rating) ? null : Convert.ToInt32(r.Rating),
            Fuente = "Web"
        }).ToList();
    }

    private List<ComentarioCsv> LeerSocial(string path)
    {
        using var reader = new StreamReader(path, Encoding.UTF8);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

        var records = csv.GetRecords<dynamic>().ToList();

        return records.Select(r => new ComentarioCsv
        {
            IdComentario = r.IdComment,
            IdClients = r.IdCliente,
            IdProducto = r.IdProducto,
            Fecha = Convert.ToDateTime(r.Fecha),
            Comentario = r.Comentario,
            Fuente = "Red Social"
        }).ToList();
    }

    private List<ComentarioCsv> LeerSurveys(string path)
    {
        using var reader = new StreamReader(path, Encoding.UTF8);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

        var records = csv.GetRecords<dynamic>().ToList();

        return records.Select(r => new ComentarioCsv
        {
            IdComentario = r.IdOpinion,
            IdClients = r.IdCliente,
            IdProducto = r.IdProducto,
            Fecha = Convert.ToDateTime(r.Fecha),
            Comentario = r.Comentario,
            PuntajeSatisfaccion = string.IsNullOrEmpty(r.PuntajeSatisfacción)
                ? null
                : Convert.ToInt32(r.PuntajeSatisfacción),
            Clasificacion = r.Clasificación,
            Fuente = "Survey"
        }).ToList();
    }

    private List<ComentarioCsv> LimpiarComentarios(List<ComentarioCsv> comentarios)
    {
        return comentarios
            .Where(c => !string.IsNullOrWhiteSpace(c.Comentario))
            .Select(c =>
            {
                c.Comentario = c.Comentario.Trim();
                return c;
            })
            .ToList();
    }

    private List<ProductoCsv> LeerProductos(string path)
    {
        using var reader = new StreamReader(path, Encoding.UTF8);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

        return csv.GetRecords<ProductoCsv>().ToList();
    }


    private async Task InsertarClientes(
        List<ComentarioCsv> comentarios,
        SqlConnection connection,
        SqlTransaction transaction)
    {
        var clientesUnicos = comentarios
            .Select(c => c.IdClients)
            .Distinct()
            .ToList();

        foreach (var idCliente in clientesUnicos)
        {
            var command = new SqlCommand(@"
IF NOT EXISTS (SELECT 1 FROM Clients WHERE IdCliente = @IdCliente)
BEGIN
    INSERT INTO Clients (IdCliente, Nombre, Email)
    VALUES (@IdCliente, @Nombre, @Email)
END
", connection, transaction); // 

            command.Parameters.AddWithValue("@IdCliente", idCliente);
            command.Parameters.AddWithValue("@Nombre", $"Cliente {idCliente}");
            command.Parameters.AddWithValue("@Email", $"cliente{idCliente}@email.com");

            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task InsertarProductos(
        List<ComentarioCsv> comentarios,
        List<ProductoCsv> productos,
        SqlConnection connection,
        SqlTransaction transaction)
    {
        var productosDesdeComentarios = comentarios
            .Select(c => c.IdProducto.Trim())
            .Distinct()
            .ToList();

        foreach (var id in productosDesdeComentarios)
        {
            var productoCsv = productos
                .FirstOrDefault(p => p.IdProducto.Trim() == id);

            string nombre = productoCsv?.Nombre.Trim() ?? $"Producto {id}";
            string categoria = productoCsv?.Categoria.Trim() ?? "Sin categoria";

            var command = new SqlCommand(@"
IF NOT EXISTS (SELECT 1 FROM Products WHERE IdProducto = @IdProducto)
BEGIN
    INSERT INTO Products (IdProducto, NombreProducto, Categoria)
    VALUES (@IdProducto, @NombreProducto, @Categoria)
END
", connection, transaction);

            command.Parameters.AddWithValue("@IdProducto", id);
            command.Parameters.AddWithValue("@NombreProducto", nombre);
            command.Parameters.AddWithValue("@Categoria", categoria);

            await command.ExecuteNonQueryAsync();
        }
    }

    public async Task InsertarEnBaseDeDatos(
        List<ComentarioCsv> comentarios,
        List<ProductoCsv> productos)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var transaction = connection.BeginTransaction();

        try
        {
            await InsertarClientes(comentarios, connection, transaction);
            await InsertarProductos(comentarios,productos, connection, transaction);

            foreach (var c in comentarios)
            {
                var command = new SqlCommand(@"
INSERT INTO Comentarios
(IdComentario, IdCliente, IdProducto, Fecha, Comentario, Rating, PuntajeSatisfaccion, Clasificacion, Fuente)
VALUES
(@IdComentario, @IdCliente, @IdProducto, @Fecha, @Comentario, @Rating, @PuntajeSatisfaccion, @Clasificacion, @Fuente)
", connection, transaction); 

                command.Parameters.AddWithValue("@IdComentario", c.IdComentario);
                command.Parameters.AddWithValue("@IdCliente", c.IdClients);
                command.Parameters.AddWithValue("@IdProducto", c.IdProducto);
                command.Parameters.AddWithValue("@Fecha", c.Fecha);
                command.Parameters.AddWithValue("@Comentario", c.Comentario);
                command.Parameters.AddWithValue("@Rating", (object?)c.Rating ?? DBNull.Value);
                command.Parameters.AddWithValue("@PuntajeSatisfaccion", (object?)c.PuntajeSatisfaccion ?? DBNull.Value);
                command.Parameters.AddWithValue("@Clasificacion", (object?)c.Clasificacion ?? DBNull.Value);
                command.Parameters.AddWithValue("@Fuente", c.Fuente);

                await command.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync();
            Console.WriteLine($" Error: {ex.Message}");
            throw;
        }
    }
}