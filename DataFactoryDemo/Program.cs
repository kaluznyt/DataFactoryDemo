using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Microsoft.Rest;
using Microsoft.Azure.Management.ResourceManager;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace DataFactoryDemo
{
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

        static void Main(string[] args)
        {
            InitializeConfigurationSettings();

            client = InitializeDataFactoryClient();

            CreateDataFactory();

            CreateLinkedService();

            CreateDataSet();

            CreatePipeline();

            ExtractRunDetails(MonitorPipeline(RunPipeline()));

            DeleteDataFactory();
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
