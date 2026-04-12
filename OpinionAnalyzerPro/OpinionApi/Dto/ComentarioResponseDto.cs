namespace OpinionApi.Dto;

public class ComentarioResponseDto
{
    public int Id { get; set; }
    public string IdComentario { get; set; } = string.Empty;
    public string? Comentario { get; set; }
    public DateTime Fecha { get; set; }
    public int? Rating { get; set; }
    public int? PuntajeSatisfaccion { get; set; }
    public string? Clasificacion { get; set; }
    public string Fuente { get; set; } = string.Empty;
    public string? Cliente { get; set; }
    public string? Email { get; set; }
    public string? NombreProducto { get; set; }
    public string? Categoria { get; set; }
}