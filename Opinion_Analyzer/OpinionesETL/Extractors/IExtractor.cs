using OpinionesETL.Models;

namespace OpinionesETL.Extractors;

/// <summary>
/// Contrato base para todos los extractores de datos.
/// Permite agregar nuevas fuentes sin modificar el Worker principal (principio Open/Closed).
/// </summary>
public interface IExtractor
{
    /// <summary>
    /// Extrae las opiniones de la fuente correspondiente de forma asíncrona.
    /// </summary>
    Task<IEnumerable<OpinionRaw>> ExtractAsync(CancellationToken cancellationToken = default);
}
