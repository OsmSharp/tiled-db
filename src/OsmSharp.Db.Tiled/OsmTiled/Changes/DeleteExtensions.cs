namespace OsmSharp.Db.Tiled.OsmTiled.Changes
{
    internal static class DeleteExtensions
    {
        public static OsmGeo CloneAsDeleted(this OsmGeo osmGeo)
        {
            return osmGeo switch
            {
                Node node => new Node() {Id = osmGeo.Id},
                Way way => new Way() {Id = osmGeo.Id},
                _ => new Relation() {Id = osmGeo.Id}
            };
        }

        public static bool IsDeleted(this OsmGeo osmGeo)
        {
            if (osmGeo.Version == null) return true;

            return false;
        }
    }
}