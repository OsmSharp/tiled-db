using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Serialization;
using OsmSharp.Changesets;

namespace OsmSharp.Db.Tiled.Replication
{
    public static class ReplicationConfigExtensions
    {
        private static readonly ThreadLocal<HttpClient> ThreadLocalClient =
            new ThreadLocal<HttpClient>(() => new HttpClient());
        private static readonly ThreadLocal<XmlSerializer> ThreadLocalXmlSerializer =
            new ThreadLocal<XmlSerializer>(() => new XmlSerializer(typeof(OsmChange)));
        
        /// <summary>
        /// Gets the latest state url for the given configuration.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <returns>The latest state url.</returns>
        internal static string LatestStateUrl(this ReplicationConfig config)
        {
            return new Uri(new Uri(config.Url), "state.txt").ToString();
        }

        /// <summary>
        /// Gets the latest replication state.
        /// </summary>
        /// <param name="config">The replication config.</param>
        /// <param name="client">A http-client to use, if any.</param>
        /// <returns>The latest replication state.</returns>
        public static async Task<ReplicationState> GetLatestReplicationState(this ReplicationConfig config, HttpClient client = null)
        {
            if (client == null) client = ThreadLocalClient.Value;
            using (var stream = await client.GetStreamAsync(config.LatestStateUrl()))
            using (var streamReader = new StreamReader(stream))
            {
                return streamReader.ParseReplicationState();
            }
        }

        /// <summary>
        /// Gets the url for the diff associated with the given replication state.
        /// </summary>
        /// <param name="config">The replication config.</param>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <returns>The url to download the diff at.</returns>
        internal static string DiffUrl(this ReplicationConfig config, long sequenceNumber)
        {
            var sequenceNumberString =  "000000000" + sequenceNumber;
            sequenceNumberString = sequenceNumberString.Substring(sequenceNumberString.Length - 9);
            var folder1 = sequenceNumberString.Substring(0, 3);
            var folder2 = sequenceNumberString.Substring(3, 3);
            var name = sequenceNumberString.Substring(6, 3);
            return new Uri(new Uri(config.Url), $"{folder1}/{folder2}/{name}.osc.gz").ToString();
        }

        /// <summary>
        /// Downloads the diff associated with the given replication state.
        /// </summary>
        /// <param name="config">The replication config.</param>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <param name="client">A http-client to use, if any.</param>
        /// <returns>The raw diff stream.</returns>
        internal static async Task<Stream> DownloadDiffStream(this ReplicationConfig config, long sequenceNumber, HttpClient client = null)
        {
            if (client == null) client = ThreadLocalClient.Value;
            return await client.GetStreamAsync(config.DiffUrl(sequenceNumber));
        }

        /// <summary>
        /// Downloads the diff associated with the given replication state.
        /// </summary>
        /// <param name="config">The replication config.</param>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <param name="client">A http-client to use, if any.</param>
        /// <returns>The diff.</returns>
        public static async Task<OsmChange> DownloadDiff(this ReplicationConfig config, long sequenceNumber,
            HttpClient client = null)
        {
            using (var stream = await config.DownloadDiffStream(sequenceNumber, client))
            using (var decompressed = new GZipStream(stream, CompressionMode.Decompress))
            using (var streamReader = new StreamReader(decompressed))
            {
                var serializer = ThreadLocalXmlSerializer.Value;
                return serializer.Deserialize(streamReader) as OsmChange;
            }
        }

        /// <summary>
        /// Gets an enumerator to loop over incoming diffs.
        /// </summary>
        /// <param name="config">The replication config.</param>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <returns>An enumerator.</returns>
        public static ReplicationChangesetEnumerator GetDiffEnumerator(this ReplicationConfig config,
            long? sequenceNumber = null)
        {
            return new ReplicationChangesetEnumerator(config, sequenceNumber);
        }
    }
}