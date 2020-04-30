using System;

namespace OsmSharp.Db.Tiled
{
    internal static class DateTimeExtensions
    {       
        /// <summary>
        /// Ticks since 1/1/1970
        /// </summary>
        private static readonly long EpochTicks = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

        /// <summary>
        /// Converts a number of milliseconds from 1/1/1970 into a standard DateTime.
        /// </summary>
        public static DateTime FromUnixTime(this long seconds)
        {
            return new DateTime(EpochTicks + (seconds * 1000));
        }

        /// <summary>
        /// Converts a standard DateTime into the number of milliseconds since 1/1/1970.
        /// </summary>
        public static long ToUnixTime(this DateTime date)
        {
            return (date.Ticks - EpochTicks) / 1000;
        }
    }
}