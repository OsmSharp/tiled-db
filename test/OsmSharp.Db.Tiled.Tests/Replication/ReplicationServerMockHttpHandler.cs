using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO.Http;

namespace OsmSharp.Db.Tiled.Tests.Replication
{
    public class ReplicationServerMockHttpHandler : IHttpHandler
    {
        public Task<Stream> TryGetStreamAsync(string requestUri)
        {
            var relativePath = requestUri.Replace("https://planet.openstreetmap.org/", string.Empty);

            var file = "./data/" + relativePath;
            if (!File.Exists(file)) return Task.FromResult<Stream>(null);
            
            return Task.FromResult<Stream>(File.OpenRead(file));
        }
    }
}