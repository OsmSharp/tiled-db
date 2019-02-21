using System.Collections.Generic;
using System.IO;
using OsmSharp.Streams;

namespace OsmSharp.Db.Tiled.Tests.Functional
{
    public static class TestExtensions
    {
        public static void WriteToOsmXml(this IEnumerable<OsmGeo> osmGeos, string file)
        {
            using (var stream = File.Open(file, FileMode.Create))
            {
                var xmlTarget = new XmlOsmStreamTarget(stream);
                xmlTarget.Initialize();
                xmlTarget.RegisterSource(osmGeos);
                xmlTarget.Pull();
                xmlTarget.Close();
            }
        }
    }
}