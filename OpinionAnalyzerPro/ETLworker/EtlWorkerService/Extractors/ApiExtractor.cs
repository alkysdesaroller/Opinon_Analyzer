using System.Net.Http.Json;
using EtlWorkerService.Models;

namespace EtlWorkerService.Extractors;

public class ApiExtractor(
    IHttpClientFactory httpClientFactory,
    IConfiguration configuration,
    ILogger<ApiExtractor> logger)
{
    private readonly ILogger<ApiExtractor> _logger = logger;
    private readonly HttpClient _httpClient = httpClientFactory.CreateClient("OpinionApi");

    private readonly string _apiUrl = configuration["Etl:ApiUrl"]
                                      ?? throw new ArgumentNullException(
                                          // ReSharper disable once NotResolvedInText
                                          "Etl:ApiUrl no está configurado en appsettings.json");

    public async Task<IEnumerable<ComentarioApi>> ExtractComentariosAsync()
    {
        try
        {
            _logger.LogInformation($"Consumiendo Api: {_apiUrl}");

            var response = await _httpClient.GetAsync(_apiUrl);
            response.EnsureSuccessStatusCode();

            var comentarios = await response.Content.ReadFromJsonAsync<List<ComentarioApi>>();
            
            // ReSharper disable once ConstantConditionalAccessQualifier
            _logger.LogInformation($"API REST: {comentarios!.Count} comentarios extraidos", comentarios?.Count ?? 0);
            return comentarios ?? [];
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "Error de conexión con la API en {Url}", _apiUrl);
            return [];
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inesperado al consumir la API");
            return [];
        }
    }
}