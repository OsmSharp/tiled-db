using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace OsmSharp.Db.Tiled.IO.Http
{
    public class HttpHandler : IHttpHandler
    {
        internal static readonly ThreadLocal<HttpClient> ThreadLocalClient =
            new ThreadLocal<HttpClient>(() => new HttpClient());
        internal static readonly Lazy<IHttpHandler> LazyHttpHandler = 
            new Lazy<IHttpHandler>(() => new HttpHandler());
        
        public Task<Stream> GetStreamAsync(string requestUri)
        {
            throw new System.NotImplementedException();
        }

        private static IHttpHandler _defaultHandler;

        /// <summary>
        /// Gets or sets the default http handler.
        /// </summary>
        public static IHttpHandler Default
        {
            get => _defaultHandler ?? (_defaultHandler = LazyHttpHandler.Value);
            set => _defaultHandler = value;
        }
    }
}