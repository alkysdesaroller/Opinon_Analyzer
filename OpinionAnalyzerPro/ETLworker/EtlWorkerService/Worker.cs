using System.Diagnostics;
using EtlWorkerService.Extractors;
using EtlWorkerService.Staging;

namespace EtlWorkerService;


    public class Worker(
        ILogger<Worker> logger,
        CsvExtractor csvExtractor,
        DatabaseExtractor databaseExtractor,
        ApiExtractor apiExtractor,
        StagingLoader stagingLoader,
        IConfiguration configuration) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var intervalMinutes = configuration.GetValue("Etl:IntervalMinutes", 60);

            logger.LogInformation("ETL Worker iniciado. Intervalo: {Interval} minuto(s)", intervalMinutes);

            while (!stoppingToken.IsCancellationRequested)
            {
                await RunEtlAsync();
                await Task.Delay(TimeSpan.FromMinutes(intervalMinutes), stoppingToken);
            }
        }

        private async Task RunEtlAsync()
        {
            var sw = Stopwatch.StartNew();
            logger.LogInformation("═══ Iniciando proceso ETL: {Time} ═══", DateTime.Now);

            // Ejecutar todas las extracciones en paralelo
            var csvTask = Task.Run(RunCsvExtractionsAsync);
            var dbTask = RunDatabaseExtractionAsync();
            var apiTask = RunApiExtractionAsync();

            await Task.WhenAll(csvTask, dbTask, apiTask);

            sw.Stop();
            logger.LogInformation("═══ Proceso ETL completado en {Elapsed}ms ═══", sw.ElapsedMilliseconds);
        }

        // ─── CSV ────────────────────────────────────────────────────────────────────

        private async Task RunCsvExtractionsAsync()
        {
            logger.LogInformation("── Extracción CSV iniciada ──");
            var sw = Stopwatch.StartNew();

            try
            {
                var clients = csvExtractor.ExtractClients();
                var fuenteDatos = csvExtractor.ExtractFuenteDatos();
                var products = csvExtractor.ExtractProducts();
                var socialComments = csvExtractor.ExtractSocialComments();
                var surveys = csvExtractor.ExtractSurveysPart1();
                var webReviews = csvExtractor.ExtractWebReviews();

                await Task.WhenAll(
                    stagingLoader.LoadClientsAsync(clients),
                    stagingLoader.LoadFuenteDatosAsync(fuenteDatos),
                    stagingLoader.LoadProductsAsync(products),
                    stagingLoader.LoadSocialCommentsAsync(socialComments),
                    stagingLoader.LoadSurveysAsync(surveys),
                    stagingLoader.LoadWebReviewsAsync(webReviews)
                );

                sw.Stop();
                logger.LogInformation("── Extracción CSV completada en {Elapsed}ms ──", sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error en extracción CSV");
            }
        }

        // ─── Base de datos relacional ───────────────────────────────────────────────

        private async Task RunDatabaseExtractionAsync()
        {
            logger.LogInformation("── Extracción BD Relacional iniciada ──");
            var sw = Stopwatch.StartNew();

            try
            {
                var comentarios = await databaseExtractor.ExtractComentariosAsync();
                await stagingLoader.LoadComentariosAsync(comentarios, "Staging_Comentarios_DB");

                sw.Stop();
                logger.LogInformation("── Extracción BD Relacional completada en {Elapsed}ms ──",
                    sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error en extracción de BD Relacional");
            }
        }

        // ─── API REST ───────────────────────────────────────────────────────────────

        private async Task RunApiExtractionAsync()
        {
            logger.LogInformation("── Extracción API REST iniciada ──");
            var sw = Stopwatch.StartNew();

            try
            {
                var comentarios = await apiExtractor.ExtractComentariosAsync();
                await stagingLoader.LoadComentariosAsync(comentarios, "Staging_Comentarios_API");

                sw.Stop();
                logger.LogInformation("── Extracción API REST completada en {Elapsed}ms ──", sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error en extracción de API REST");
            }
        }
    }
    