using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Rest;

namespace DataFactoryDemo
{
    public class AzureDataFactoryClient : IAzureDataFactoryClient
    {
        private readonly IAzureAppAuthenticationProvider _azureAppAuthenticationProvider;

        private readonly string _subscriptionId;
        private readonly string _resourceGroup;
        private readonly string _dataFactoryName;

        private DataFactoryManagementClient _dataFactoryClient;

        public AzureDataFactoryClient(IAzureAppAuthenticationProvider azureAppAuthenticationProvider, string subscriptionId, string resourceGroup, string dataFactoryName)
        {
            _azureAppAuthenticationProvider = azureAppAuthenticationProvider;
            _resourceGroup = resourceGroup;
            _dataFactoryName = dataFactoryName;
            _subscriptionId = subscriptionId;

            InitializeClient().Wait();
        }

        private async Task InitializeClient()
        {
            var token = await _azureAppAuthenticationProvider.LoginAsync();
            var tokenCredentials = new TokenCredentials(token);

            _dataFactoryClient = new DataFactoryManagementClient(tokenCredentials) { SubscriptionId = _subscriptionId };

            await Task.CompletedTask;
        }

        public async Task<string> RunPipelineAsync(string pipelineName, PipelineParameters pipelineParameters)
        {
            return await Task.FromResult((await _dataFactoryClient.Pipelines
                    .CreateRunWithHttpMessagesAsync(
                        _resourceGroup,
                        _dataFactoryName,
                        pipelineName,
                        parameters: pipelineParameters.ToDictionary()))
                .Body.RunId);
        }

        public async Task<string> GetPipelineRunStatusAsync(string pipelineRunId)
        {
            return await Task.FromResult((await _dataFactoryClient.PipelineRuns.GetAsync(_resourceGroup, _dataFactoryName, pipelineRunId)).Status);
        }
    }
}