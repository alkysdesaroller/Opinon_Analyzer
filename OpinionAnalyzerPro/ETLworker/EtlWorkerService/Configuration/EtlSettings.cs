namespace EtlWorkerService.Configuration;

public class EtlSettings
{
    public string CsvFolder { get; set; } = string.Empty;
    public string ApiUrl { get; set; } = string.Empty;
    public int IntervalMinutes { get; set; }
}