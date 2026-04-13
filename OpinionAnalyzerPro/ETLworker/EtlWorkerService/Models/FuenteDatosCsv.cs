using CsvHelper.Configuration.Attributes;

namespace EtlWorkerService.Models;

public class FuenteDatosCsv
{
    [Name("IdFuente")]
    public string IdFuente { get; set; } = string.Empty;
 
    [Name("TipoFuente")]
    public string TipoFuente { get; set; } = string.Empty;
 
    [Name("FechaCarga")]
    public string FechaCarga { get; set; } = string.Empty;
}
