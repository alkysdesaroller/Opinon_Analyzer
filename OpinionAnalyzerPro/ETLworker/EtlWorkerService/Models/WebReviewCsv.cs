using CsvHelper.Configuration.Attributes;

namespace EtlWorkerService.Models;

public class WebReviewCsv
{
    [Name("IdReview")]
    public string IdReview { get; set; } = string.Empty;
 
    [Name("IdCliente")]
    public string IdCliente { get; set; } = string.Empty;
 
    [Name("IdProducto")]
    public string IdProducto { get; set; } = string.Empty;
 
    [Name("Fecha")]
    public string Fecha { get; set; } = string.Empty;
 
    [Name("Comentario")]
    public string Comentario { get; set; } = string.Empty;
 
    [Name("Rating")]
    public string Rating { get; set; } = string.Empty;
}