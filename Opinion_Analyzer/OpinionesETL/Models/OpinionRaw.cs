namespace OpinionesETL.Models;

/// <summary>
/// Representa una opinión cruda extraída de cualquier fuente antes de la transformación.
/// </summary>
public class OpinionRaw
{
    public string SourceType { get; set; } = string.Empty;  // "encuesta" | "review" | "social"
    public string? ProductId { get; set; }
    public string? CustomerId { get; set; }
    public string? CustomerEmail { get; set; }
    public string? CustomerName { get; set; }
    public string? Country { get; set; }
    public int? Rating { get; set; }
    public string? CommentText { get; set; }
    public DateTime? OpinionDate { get; set; }

    // CSV (encuesta)
    public string? Classification { get; set; }   // Positiva / Negativa / Neutra
    public int? Score { get; set; }

    // API REST (social)
    public string? Platform { get; set; }         // Instagram / Twitter / etc.
    public string? Sentiment { get; set; }        // positivo / negativo / neutro
}
