namespace OpinionApi.Models;

public class ComentarioEntity
{
    public int Id { get; set; }               // PK
    public int IdComentario { get; set; }
    public int IdCliente { get; set; }
    public int IdProducto { get; set; }

    public DateTime Fecha { get; set; }
    public string Comentario { get; set; }

    public int? Rating { get; set; }
    public int? PuntajeSatisfaccion { get; set; }

    public string? Clasificacion { get; set; }
    public string Fuente { get; set; }
}