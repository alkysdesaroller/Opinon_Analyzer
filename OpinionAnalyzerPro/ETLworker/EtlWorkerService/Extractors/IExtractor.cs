namespace EtlWorkerService.Extractors;

public interface IExtractor<T>
{
    Task<IEnumerable<T>> ExtractAsync();
}