using System.Threading.Tasks;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace DataFactoryDemo
{
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
}