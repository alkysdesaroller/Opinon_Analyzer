namespace OpinionesETL.Configuration;

/// <summary>
/// Configuración centralizada de todas las fuentes y credenciales.
/// Se carga desde appsettings.json (sección "ExtractorSettings").
/// En producción, los valores sensibles deben sobreescribirse con variables de entorno.
/// </summary>
public class ExtractorSettings
{
    // CSV
    public string CsvFilePath { get; set; } = "data/encuestas.csv";

    // SQL Server (fuente relacional)
    public string DatabaseConnectionString { get; set; } = string.Empty;

    // BD Analítica
    public string AnalyticConnectionString { get; set; } = string.Empty;

    // API REST
    public string ApiBaseUrl { get; set; } = string.Empty;
    public string ApiKey { get; set; } = string.Empty;

    // Scheduler
    public int IntervalMinutes { get; set; } = 60;
}
