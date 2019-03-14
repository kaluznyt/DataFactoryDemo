using System;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Rest;

namespace DataFactoryDemo
{
    public class AzureDataFactoryClient : IAzureDataFactoryClient
    {
        private readonly IAzureAuthenticationProvider _azureAuthenticationProvider;

        private readonly string _subscriptionId;
        private readonly string _resourceGroup;
        private readonly string _dataFactoryName;

        private DataFactoryManagementClient _dataFactoryClient;

        public AzureDataFactoryClient(IAzureAuthenticationProvider azureAuthenticationProvider, string subscriptionId, string resourceGroup, string dataFactoryName)
        {
            _azureAuthenticationProvider = azureAuthenticationProvider;
            _resourceGroup = resourceGroup;
            _dataFactoryName = dataFactoryName;
            _subscriptionId = subscriptionId;

            InitializeClient().Wait(); 
        }

        private async Task InitializeClient()
        {
            var token = await _azureAuthenticationProvider.LoginAsync();
            var tokenCredentials = new TokenCredentials(token);
            _dataFactoryClient = new DataFactoryManagementClient(tokenCredentials) { SubscriptionId = _subscriptionId };

            await Task.CompletedTask;
        }

        public async Task<string> RunPipelineAsync(string pipelineName, PipelineParameters pipelineParameters)
        {
            Console.WriteLine("Running async");
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
            return (await _dataFactoryClient.PipelineRuns.GetAsync(_resourceGroup, _dataFactoryName, pipelineRunId)).Status;
        }
    }
}