using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO.Http;

namespace OsmSharp.Db.Tiled.Tests.Replication
{
    public class ReplicationServerMockHttpHandler : IHttpHandler
    {
        public Task<Stream> GetStreamAsync(string requestUri)
        {
            var relativePath = requestUri.Replace("https://planet.openstreetmap.org/", string.Empty);

            var stream = File.OpenRead("./data/" + relativePath);

            return Task.FromResult<Stream>(stream);
        }
    }
}