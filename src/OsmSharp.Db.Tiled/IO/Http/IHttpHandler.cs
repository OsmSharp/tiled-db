using System.IO;
using System.Threading.Tasks;

namespace OsmSharp.Db.Tiled.IO.Http
{
    /// <summary>
    /// Abstract representation of an http handler.
    /// </summary>
    public interface IHttpHandler
    {
        /// <summary>
        /// Gets a stream representing the data at the given url.
        /// </summary>
        /// <param name="requestUri">The uri.</param>
        /// <returns>The stream.</returns>
        Task<Stream> TryGetStreamAsync(string requestUri);
    }
}