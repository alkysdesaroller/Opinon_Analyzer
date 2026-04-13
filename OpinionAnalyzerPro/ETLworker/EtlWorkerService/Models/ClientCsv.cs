using CsvHelper.Configuration.Attributes;

namespace EtlWorkerService.Models;

public class ClientCsv
{
 [Name("IdCliente")]
 public string IdCliente { get; set; } = string.Empty;
 
 [Name("Nombre")]
 public string Nombre { get; set; } = string.Empty;
 
 [Name("Email")]
 public string Email { get; set; } = string.Empty;
}