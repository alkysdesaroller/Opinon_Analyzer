using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using EtlWorkerService.Configuration;
using EtlWorkerService.Models;
using Microsoft.Extensions.Options;

namespace EtlWorkerService.Extractors;

public class CsvExtractor(
    ILogger<CsvExtractor> logger,
    IOptions<EtlSettings> settings)
{
    private readonly string _csvFolder = settings.Value.CsvFolder;

    private IEnumerable<T> ReadCsv<T>(string fileName)
    {
        var filePath = Path.Combine(_csvFolder, fileName);

        if (!File.Exists(filePath))
        {
            logger.LogWarning($"Archivo CSV no encontrado: {filePath}");
            return Enumerable.Empty<T>();
        }

        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Encoding = Encoding.UTF8,
            HasHeaderRecord = true,
            MissingFieldFound = null,
            BadDataFound = context =>
                logger.LogWarning("Dato inválido en {File} fila {Row}: {Field}",
                    fileName, context.Context.Parser!.Row, context.Field)
        };

        using var reader = new StreamReader(filePath, Encoding.UTF8);
        using var csv = new CsvReader(reader, config);

        try
        {
            var records = csv.GetRecords<T>().ToList();
            logger.LogInformation("CSV {File}: {Count} registros extraídos", fileName, records.Count);
            return records;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error leyendo CSV {File}", fileName);
            return Enumerable.Empty<T>();
        }
    }

    public IEnumerable<ClientCsv> ExtractClients()
        => ReadCsv<ClientCsv>("clients.csv");

    public IEnumerable<FuenteDatosCsv> ExtractFuenteDatos()
        => ReadCsv<FuenteDatosCsv>("fuente_datos.csv");

    public IEnumerable<ProductCsv> ExtractProducts()
        => ReadCsv<ProductCsv>("products.csv");

    public IEnumerable<SocialCommentCsv> ExtractSocialComments()
        => ReadCsv<SocialCommentCsv>("social_comments.csv");

    public IEnumerable<SurveyPart1Csv> ExtractSurveysPart1()
        => ReadCsv<SurveyPart1Csv>("surveys_part1.csv");

    public IEnumerable<WebReviewCsv> ExtractWebReviews()
        => ReadCsv<WebReviewCsv>("web_reviews.csv");
}