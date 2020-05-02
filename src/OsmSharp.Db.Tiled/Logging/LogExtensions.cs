using System;

namespace OsmSharp.Db.Tiled.Logging
{
    internal static class LogExtensions
    {
        public static ProgressRelative ProgressRelative(this Log log, TraceEventType type = TraceEventType.Verbose,
            Func<int, string>? getMessage = null)
        {
            return new ProgressRelative(log, type, getMessage);
        }
        
        public static ProgressAbsolute ProgressAbsolute(this Log log, TraceEventType type = TraceEventType.Verbose,
            Func<long, string>? getMessage = null, int increments = 1000)
        {
            return new ProgressAbsolute(log, type, getMessage, increments);
        }
    }
}