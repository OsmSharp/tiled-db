using System;
using System.IO;
using Serilog;
using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.Tests.Functional
{
    class Program
    {
        static void Main(string[] args)
        {
            args = new string[]
            {
                @"C:\work\data\OSM\belgium-latest.osm.pbf",
                @"C:\work\itinero\tiled-osm-db\db",
                @"C:\work\itinero\tiled-osm-db\complete"
            };

            uint zoom = 14;

            OsmSharp.Logging.Logger.LogAction = (o, level, message, parameters) =>
            {
                Console.WriteLine(string.Format("[{0}] {1} - {2}", o, level, message));
            };

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.LiterateConsole()
                .CreateLogger();

            // build db.
            var source = new OsmSharp.Streams.PBFOsmStreamSource(
                File.OpenRead(args[0]));
            var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
            progress.RegisterSource(source);

            // building database.
            var ticks = DateTime.Now.Ticks;
            Build.Builder.Build(progress, args[1], zoom);
            var span = new TimeSpan(DateTime.Now.Ticks - ticks);
            Console.WriteLine("Splitting took {0}s", span);

            // reading some data.
            var db = new Database(args[1]);
            
            // write some complete tiles.
            if (!Directory.Exists(args[2]))
            {
                throw new Exception();
            }
            foreach (var baseTile in db.GetTiles())
            {
                Console.WriteLine("Base tile found: {0}", baseTile);

                var file = Path.Combine(args[2], baseTile.Zoom.ToInvariantString(), baseTile.X.ToInvariantString(),
                    baseTile.Y.ToInvariantString() + ".osm.pbf");
                var fileInfo = new FileInfo(file);
                if (!fileInfo.Directory.Exists)
                {
                    fileInfo.Directory.Create();
                }

                using (var stream = File.Open(file, FileMode.Create))
                {
                    var target = new OsmSharp.Streams.PBFOsmStreamTarget(stream);
                    target.Initialize();

                    db.GetCompleteTile(baseTile, target);

                    target.Flush();
                    target.Close();
                }
            }

            Console.ReadLine();
        }
    }
}