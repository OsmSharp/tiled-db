using Anyways.Osm.TiledDb.Collections;
using Anyways.Osm.TiledDb.IO.Binary;
using Anyways.Osm.TiledDb.IO.PBF;
using OsmSharp;
using OsmSharp.IO.Xml;
using OsmSharp.Streams;
using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using System.Xml.Serialization;

namespace Anyways.Osm.TiledDb.Tests.Functional
{
    public class Program
    {
        public static void Main(string[] args)
        {            
            // enable logging.
            OsmSharp.Logging.Logger.LogAction = (o, level, message, parameters) =>
            {
                System.Console.WriteLine(string.Format("[{0}] {1} - {2}", o, level, message));
            };

            //var zoom = 14;

            var source = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
            source.RegisterSource(new PBFOsmStreamSource(File.OpenRead(@"C:\work\anyways\tiled-osm-db\belgium-highways.osm.pbf")));

            var target = new BinaryOsmStreamTarget(File.OpenWrite(@"C:\work\anyways\tiled-osm-db\belgium-highways.osm.bin"));
            target.RegisterSource(source);
            target.Pull();

        }
    }
}
