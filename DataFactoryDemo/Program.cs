using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Serialization;

namespace DataFactoryDemo
{

    public interface IAzureDataFactoryClient
    {
        Task<string> RunPipelineAsync(string pipelineName, PipelineParameters pipelineParameters);

        Task<string> GetPipelineRunStatusAsync(string pipelineRunId);
    }

    public interface IAzureAuthenticationProvider
    {
        Task<string> LoginAsync();
    }

    public class AzureAuthenticationProvider : IAzureAuthenticationProvider
    {
        private readonly string _tenantId;
        private readonly string _applicationId;
        private readonly string _authenticationKey;

        private const string AzureLoginUrl = "https://login.windows.net/";
        private const string AzureManagementUrl = "https://management.azure.com/";

        public AzureAuthenticationProvider(string tenantId, string applicationId, string authenticationKey)
        {
            this._tenantId = tenantId;
            this._applicationId = applicationId;
            this._authenticationKey = authenticationKey;
        }

        public async Task<string> LoginAsync()
        {
            var context = new AuthenticationContext(AzureLoginUrl + _tenantId);
            var cc = new ClientCredential(_applicationId, _authenticationKey);
            return await Task.FromResult((await context.AcquireTokenAsync(AzureManagementUrl, cc)).AccessToken);
        }
    }

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

    public class PipelineParameters
    {
        public string InputPath { get; set; }
        public string OutputPath { get; set; }

        public Dictionary<string, object> ToDictionary()
        {
            return new Dictionary<string, object>
            {
                [nameof(InputPath).ToLower()] = InputPath,
                [nameof(OutputPath).ToLower()] = OutputPath
            };
        }
    }

    class Program
    {
        private static string tenantId;
        private static string applicationId;
        private static string authenticationKey;
        private static string subscriptionId;
        private static string resourceGroup;
        private static string dataFactoryName;
        private static string storageAccount;
        private static string storageKey;

        private static string region = "North Europe";
        static string inputBlobPath = "adftutorial/input";
        static string outputBlobPath = "adftutorial/output";
        static string storageLinkedServiceName = "AzureStorageLinkedService";
        static string blobDatasetName = "BlobDataset";
        static string pipelineName = "Adfv2QuickStartPipeline";

        private static DataFactoryManagementClient client;

        static async Task Main(string[] args)
        {
            InitializeConfigurationSettings();

            //client = InitializeDataFactoryClient();

            //CreateDataFactory();

            //CreateLinkedService();

            //CreateDataSet();

            //CreatePipeline();

            //ExtractRunDetails(MonitorPipeline(RunPipelineAsync()));

            //DeleteDataFactory();

            var azureAuthenticationProvider = new AzureAuthenticationProvider(tenantId, applicationId, authenticationKey);

            var dataFactoryClient = new AzureDataFactoryClient(azureAuthenticationProvider, subscriptionId, resourceGroup, dataFactoryName);

            Task.Factory.StartNew(async () =>
                await dataFactoryClient.RunPipelineAsync(pipelineName,
                    new PipelineParameters {InputPath = "abc", OutputPath = "cda"}));

            //Console.WriteLine(pipelineRunId);

            //var pipelineRunStatus = await dataFactoryClient.GetPipelineRunStatusAsync(pipelineRunId);

            Console.WriteLine("abc");

            Console.ReadKey();
        }

        private static void DeleteDataFactory()
        {
            Console.WriteLine("Press any key to delete data factory");
            Console.ReadKey();
            Console.WriteLine("Deleting the data factory");
            client.Factories.Delete(resourceGroup, dataFactoryName);
        }

        private static void InitializeConfigurationSettings()
        {
            tenantId = ConfigurationManager.AppSettings["tenantId"];
            applicationId = ConfigurationManager.AppSettings["applicationId"];
            authenticationKey = ConfigurationManager.AppSettings["authenticationKey"];
            subscriptionId = ConfigurationManager.AppSettings["subscriptionId"];
            resourceGroup = ConfigurationManager.AppSettings["resourceGroup"];
            dataFactoryName = ConfigurationManager.AppSettings["dataFactoryName"];
            storageAccount = ConfigurationManager.AppSettings["storageAccount"];
            storageKey = ConfigurationManager.AppSettings["storageKey"];
        }

        private static void ExtractRunDetails((PipelineRun pipelineRun, CreateRunResponse runResponse) input)
        {
            // Check the copy activity run details
            Console.WriteLine("Checking copy activity run details...");

            var activityRuns = client.ActivityRuns.QueryByPipelineRun(
                resourceGroup, dataFactoryName, input.runResponse.RunId,
                new RunFilterParameters(DateTime.UtcNow.AddMinutes(-10),
                    DateTime.UtcNow.AddMinutes(10))).Value;
            if (input.pipelineRun.Status == "Succeeded")
                Console.WriteLine(activityRuns.First().Output);
            else
                Console.WriteLine(activityRuns.First().Error);
            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }

        private static (PipelineRun, CreateRunResponse) MonitorPipeline(CreateRunResponse runResponse)
        {
            // Monitor the pipeline run
            Console.WriteLine("Checking pipeline run status...");
            PipelineRun pipelineRun;
            while (true)
            {
                pipelineRun = client.PipelineRuns.Get(resourceGroup, dataFactoryName, runResponse.RunId);
                Console.WriteLine("Status: " + pipelineRun.Status);
                if (pipelineRun.Status == "InProgress")
                    System.Threading.Thread.Sleep(15000);
                else
                {
                    return (pipelineRun, runResponse);
                }
            }
        }

        private static CreateRunResponse RunPipeline()
        {
            // Create a pipeline run
            Console.WriteLine("Creating pipeline run...");
            Dictionary<string, object> parameters = new Dictionary<string, object>
            {
                {"inputPath", inputBlobPath},
                {"outputPath", outputBlobPath}
            };
            CreateRunResponse runResponse = client.Pipelines
                .CreateRunWithHttpMessagesAsync(resourceGroup, dataFactoryName, pipelineName, parameters: parameters).Result
                .Body;
            Console.WriteLine("Pipeline run ID: " + runResponse.RunId);

            return runResponse;
        }

        private static void CreatePipeline()
        {
            // Create a pipeline with a copy activity
            Console.WriteLine("Creating pipeline " + pipelineName + "...");
            PipelineResource pipeline = new PipelineResource
            {
                Parameters = new Dictionary<string, ParameterSpecification>
                {
                    {"inputPath", new ParameterSpecification {Type = ParameterType.String}},
                    {"outputPath", new ParameterSpecification {Type = ParameterType.String}}
                },
                Activities = new List<Activity>
                {
                    new CopyActivity
                    {
                        Name = "CopyFromBlobToBlob",
                        Inputs = new List<DatasetReference>
                        {
                            new DatasetReference()
                            {
                                ReferenceName = blobDatasetName,
                                Parameters = new Dictionary<string, object>
                                {
                                    {"path", "@pipeline().parameters.inputPath"}
                                }
                            }
                        },
                        Outputs = new List<DatasetReference>
                        {
                            new DatasetReference
                            {
                                ReferenceName = blobDatasetName,
                                Parameters = new Dictionary<string, object>
                                {
                                    {"path", "@pipeline().parameters.outputPath"}
                                }
                            }
                        },
                        Source = new BlobSource { },
                        Sink = new BlobSink { }
                    }
                }
            };
            client.Pipelines.CreateOrUpdate(resourceGroup, dataFactoryName, pipelineName, pipeline);
            Console.WriteLine(SafeJsonConvert.SerializeObject(pipeline, client.SerializationSettings));
        }

        private static void CreateDataSet()
        {
            // Create an Azure Blob dataset
            Console.WriteLine("Creating dataset " + blobDatasetName + "...");
            DatasetResource blobDataset = new DatasetResource(
                new AzureBlobDataset
                {
                    LinkedServiceName = new LinkedServiceReference
                    {
                        ReferenceName = storageLinkedServiceName
                    },
                    FolderPath = new Expression { Value = "@{dataset().path}" },
                    Parameters = new Dictionary<string, ParameterSpecification>
                    {
                        {"path", new ParameterSpecification {Type = ParameterType.String}}
                    }
                }
            );
            client.Datasets.CreateOrUpdate(resourceGroup, dataFactoryName, blobDatasetName, blobDataset);
            Console.WriteLine(SafeJsonConvert.SerializeObject(blobDataset, client.SerializationSettings));
        }

        private static void CreateLinkedService()
        {
            // Create an Azure Storage linked service
            Console.WriteLine("Creating linked service " + storageLinkedServiceName + "...");

            LinkedServiceResource storageLinkedService = new LinkedServiceResource(
                new AzureStorageLinkedService
                {
                    ConnectionString = new SecureString("DefaultEndpointsProtocol=https;AccountName=" + storageAccount +
                                                        ";AccountKey=" + storageKey)
                }
            );
            client.LinkedServices.CreateOrUpdate(resourceGroup, dataFactoryName, storageLinkedServiceName,
                storageLinkedService);
            Console.WriteLine(SafeJsonConvert.SerializeObject(storageLinkedService, client.SerializationSettings));
        }

        private static void CreateDataFactory()
        {
            Console.WriteLine("Creating data factory " + dataFactoryName + "...");
            Factory dataFactory = new Factory
            {
                Location = region,
                Identity = new FactoryIdentity()
            };
            client.Factories.CreateOrUpdate(resourceGroup, dataFactoryName, dataFactory);
            Console.WriteLine(SafeJsonConvert.SerializeObject(dataFactory, client.SerializationSettings));

            while (client.Factories.Get(resourceGroup, dataFactoryName).ProvisioningState == "PendingCreation")
            {
                System.Threading.Thread.Sleep(1000);
            }
        }

        private static DataFactoryManagementClient InitializeDataFactoryClient()
        {
            var context = new AuthenticationContext("https://login.windows.net/" + tenantId);
            ClientCredential cc = new ClientCredential(applicationId, authenticationKey);
            AuthenticationResult result = context.AcquireTokenAsync("https://management.azure.com/", cc).Result;
            ServiceClientCredentials cred = new TokenCredentials(result.AccessToken);
            var client = new DataFactoryManagementClient(cred) { SubscriptionId = subscriptionId };
            return client;
        }
    }
}
