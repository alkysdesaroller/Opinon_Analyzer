using Microsoft.AspNetCore.Mvc;

namespace OpinionesApi.Controllers;

/// <summary>
/// API REST que expone los comentarios de redes sociales cargados desde CSV.
/// El ETL Worker consumirá este endpoint a través de ApiExtractor.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class CommentsController : ControllerBase
{
    private readonly ILogger<CommentsController> _logger;

    // En producción esto vendría de una BD; aquí usamos datos en memoria
    // basados en los CSV de la práctica anterior.
    private static readonly List<CommentDto> _comments = SeedData();

    public CommentsController(ILogger<CommentsController> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Devuelve todos los comentarios no procesados.
    /// El ETL los marcará como procesados después de la extracción.
    /// </summary>
    [HttpGet("unprocessed")]
    public IActionResult GetUnprocessed([FromHeader(Name = "X-Api-Key")] string? apiKey)
    {
        // Validación simple de API Key
        var expectedKey = Environment.GetEnvironmentVariable("API_KEY") ?? "dev-key-local";
        if (apiKey != expectedKey)
        {
            _logger.LogWarning("Intento de acceso con API Key inválida.");
            return Unauthorized(new { error = "API Key inválida." });
        }

        var unprocessed = _comments.Where(c => !c.IsProcessed).ToList();
        _logger.LogInformation("Devolviendo {Count} comentarios no procesados.", unprocessed.Count);
        return Ok(unprocessed);
    }

    private static List<CommentDto> SeedData() =>
    [
        new() { ProductId = "P001", CustomerId = "C001", Email = "ana@email.com",
                CustomerName = "Ana García", Country = "DO",
                CommentText = "Excelente producto, lo recomiendo.", Platform = "Instagram",
                Sentiment = "positivo", CommentDate = DateTime.Now.AddDays(-5) },

        new() { ProductId = "P002", CustomerId = "C002", Email = "juan@email.com",
                CustomerName = "Juan Pérez", Country = "DO",
                CommentText = "El envío tardó demasiado.", Platform = "Twitter",
                Sentiment = "negativo", CommentDate = DateTime.Now.AddDays(-3) },

        new() { ProductId = "P001", CustomerId = "C003", Email = "maria@email.com",
                CustomerName = "María Rodríguez", Country = "US",
                CommentText = "Calidad aceptable para el precio.", Platform = "Instagram",
                Sentiment = "neutro", CommentDate = DateTime.Now.AddDays(-1) },
    ];
}

public class CommentDto
{
    public string? ProductId    { get; set; }
    public string? CustomerId   { get; set; }
    public string? Email        { get; set; }
    public string? CustomerName { get; set; }
    public string? Country      { get; set; }
    public string? CommentText  { get; set; }
    public DateTime? CommentDate { get; set; }
    public string? Platform     { get; set; }
    public string? Sentiment    { get; set; }
    public bool IsProcessed     { get; set; } = false;
}
