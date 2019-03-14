using System.Threading.Tasks;

namespace DataFactoryDemo
{
    public interface IAzureAuthenticationProvider
    {
        Task<string> LoginAsync();
    }
}