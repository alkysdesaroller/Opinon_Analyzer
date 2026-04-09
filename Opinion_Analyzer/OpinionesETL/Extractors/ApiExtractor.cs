using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpinionesETL.Configuration;
using OpinionesETL.Models;

namespace OpinionesETL.Extractors;

/// <summary>
/// Extrae comentarios de redes sociales desde la API REST de OpinionesAPI.
/// Rendimiento: usa IHttpClientFactory (reutiliza conexiones TCP, evita socket exhaustion).
/// Seguridad: el ApiKey se lee de configuración; nunca hardcodeado.
/// </summary>
public class ApiExtractor : IExtractor
{
    private readonly ILogger<ApiExtractor> _logger;
    private readonly HttpClient _http;
    private readonly ExtractorSettings _settings;

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public ApiExtractor(ILogger<ApiExtractor> logger,
                        IHttpClientFactory httpFactory,
                        IOptions<ExtractorSettings> settings)
    {
        _logger   = logger;
        _settings = settings.Value;
        _http     = httpFactory.CreateClient("OpinionesApi");
    }

    public async Task<IEnumerable<OpinionRaw>> ExtractAsync(CancellationToken cancellationToken = default)
    {
        var sw = Stopwatch.StartNew();
        _logger.LogInformation("[ApiExtractor] Iniciando extracción desde API REST. Endpoint: {Url}",
            _settings.ApiBaseUrl);

        var results = new List<OpinionRaw>();

        try
        {
            var request = new HttpRequestMessage(HttpMethod.Get, "/api/comments/unprocessed");
            request.Headers.Add("X-Api-Key", _settings.ApiKey);

            var response = await _http.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync(cancellationToken);
            var comments = JsonSerializer.Deserialize<List<ApiCommentDto>>(json, _jsonOptions);

            if (comments is null || comments.Count == 0)
            {
                _logger.LogWarning("[ApiExtractor] La API devolvió 0 registros.");
                return results;
            }

            foreach (var c in comments)
            {
                results.Add(new OpinionRaw
                {
                    SourceType    = "social",
                    ProductId     = c.ProductId,
                    CustomerId    = c.CustomerId,
                    CustomerEmail = c.Email,
                    CustomerName  = c.CustomerName,
                    Country       = c.Country,
                    CommentText   = c.CommentText,
                    OpinionDate   = c.CommentDate,
                    Platform      = c.Platform,
                    Sentiment     = c.Sentiment
                });
            }

            sw.Stop();
            _logger.LogInformation("[ApiExtractor] Extracción completa. Registros: {Count}. Tiempo: {Ms}ms",
                results.Count, sw.ElapsedMilliseconds);
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "[ApiExtractor] Error HTTP al consumir la API. Status: {Status}", ex.StatusCode);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "[ApiExtractor] Error al deserializar respuesta de la API");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "[ApiExtractor] Error inesperado durante la extracción");
        }

        return results;
    }
}

// DTO que mapea exactamente el contrato JSON de OpinionesAPI
public class ApiCommentDto
{
    [JsonPropertyName("productId")]     public string? ProductId    { get; set; }
    [JsonPropertyName("customerId")]    public string? CustomerId   { get; set; }
    [JsonPropertyName("email")]         public string? Email        { get; set; }
    [JsonPropertyName("customerName")]  public string? CustomerName { get; set; }
    [JsonPropertyName("country")]       public string? Country      { get; set; }
    [JsonPropertyName("commentText")]   public string? CommentText  { get; set; }
    [JsonPropertyName("commentDate")]   public DateTime? CommentDate { get; set; }
    [JsonPropertyName("platform")]      public string? Platform     { get; set; }
    [JsonPropertyName("sentiment")]     public string? Sentiment    { get; set; }
}
