using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Anyways.Osm.TiledDb.Splitter
{
    public static class Extensions
    {

        /// <summary>
        /// Gets the substring until the first dot.
        /// </summary>
        public static string GetNameUntilFirstDot(this string name)
        {
            var dotIdx = name.IndexOf('.');
            if (dotIdx == 0)
            {
                throw new Exception("No '.' found in file name.");
            }
            return name.Substring(0, dotIdx);
        }
    }
}
