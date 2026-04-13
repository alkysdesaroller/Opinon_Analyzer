using CsvHelper.Configuration.Attributes;

namespace EtlWorkerService.Models;

public class ProductCsv
{
    [Name("IdProducto")]
    public string IdProducto { get; set; } = string.Empty;
 
    [Name("Nombre")]
    public string Nombre { get; set; } = string.Empty;
 
    [Name("Categoría")]
    public string Categoria { get; set; } = string.Empty;
}