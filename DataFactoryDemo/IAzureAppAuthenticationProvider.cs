using System.Threading.Tasks;

namespace DataFactoryDemo
{
    public interface IAzureAppAuthenticationProvider
    {
        Task<string> LoginAsync();
    }
}