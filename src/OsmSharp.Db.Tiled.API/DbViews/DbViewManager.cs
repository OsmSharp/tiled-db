using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.API.DbViews
{
    internal static class DbViewManager
    {
        private static readonly Dictionary<string, IDatabaseView> DatabaseViews = new Dictionary<string, IDatabaseView>();

        public static IDatabaseView Get(string view)
        {
            return null;
        }

        public static void Release(string view)
        {
            
        }
    }
}