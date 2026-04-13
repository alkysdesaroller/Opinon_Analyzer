
using Microsoft.Data.SqlClient;
using Opiones.DataSeeder.Services; // ajusta si tu namespace es diferente

var connectionString =
    "Server=MSI;Database=OpinionesTrasaccionalDb;User Id=sa;Password=alb8n765;TrustServerCertificate=True;";

// 🔎 Verificar conexión real
await using (var connection = new SqlConnection(connectionString))
{
    connection.Open();

    Console.WriteLine("=== CONEXIÓN ACTUAL ===");
    Console.WriteLine($"Servidor: {connection.DataSource}");
    Console.WriteLine($"Base de datos: {connection.Database}");
    Console.WriteLine("========================\n");
}

// Ejecutar Seeder
var seeder = new SeederService(connectionString);
 await seeder.Execute();


 await using (var connection = new SqlConnection(connectionString))
{
    connection.Open();

    var command = new SqlCommand("SELECT COUNT(*) FROM Comentarios", connection);
    var total = (int)command.ExecuteScalar();

    Console.WriteLine($"\nTotal de registros en la tabla Comentarios: {total}");
}