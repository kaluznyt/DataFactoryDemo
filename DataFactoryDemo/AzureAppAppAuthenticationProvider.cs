using System.Threading.Tasks;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace DataFactoryDemo
{
    public class AzureAppAppAuthenticationProvider : IAzureAppAuthenticationProvider
    {
        private readonly string _azureAdTenantId;
        private readonly string _applicationId;
        private readonly string _appAuthenticationKey;

        private const string AzureLoginUrl = "https://login.windows.net/";
        private const string AzureManagementUrl = "https://management.azure.com/";

        public AzureAppAppAuthenticationProvider(string azureAdTenantId, string applicationId, string appAuthenticationKey)
        {
            this._azureAdTenantId = azureAdTenantId;
            this._applicationId = applicationId;
            this._appAuthenticationKey = appAuthenticationKey;
        }

        public async Task<string> LoginAsync()
        {
            var context = new AuthenticationContext(AzureLoginUrl + _azureAdTenantId);
            var cc = new ClientCredential(_applicationId, _appAuthenticationKey);
            return await Task.FromResult((await context.AcquireTokenAsync(AzureManagementUrl, cc)).AccessToken);
        }
    }
}