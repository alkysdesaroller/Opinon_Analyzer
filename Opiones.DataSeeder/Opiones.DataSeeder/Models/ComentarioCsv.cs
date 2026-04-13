namespace Opiones.DataSeeder;

public class ComentarioCsv
{
    public string IdComentario { get; set; } = string.Empty;
    public string IdClients { get; set; } = string.Empty;
    public string IdProducto { get; set; } = string.Empty;
    
    public string Comentario { get; set; } = string.Empty;
    public DateTime Fecha { get; set; }
    public int? Rating { get; set; }
    public int? PuntajeSatisfaccion { get; set; }
    public string? Clasificacion { get; set; }
    public string Fuente { get; set; } = string.Empty;
}