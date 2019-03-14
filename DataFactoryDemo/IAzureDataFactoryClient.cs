using System.Threading.Tasks;

namespace DataFactoryDemo
{
    public interface IAzureDataFactoryClient
    {
        Task<string> RunPipelineAsync(string pipelineName, PipelineParameters pipelineParameters);

        Task<string> GetPipelineRunStatusAsync(string pipelineRunId);
    }
}