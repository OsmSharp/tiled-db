// using System.Drawing;
// using System.IO;
// using OsmSharp.Db.Tiled.IO;
// using OsmSharp.Db.Tiled.OsmTiled.IO;
//
// namespace OsmSharp.Db.Tiled.OsmTiled.Data
// {
//
//     internal class OsmTiledDbTileIndexReadOnly
//     {
//         private readonly Stream _data;
//         private readonly int _headerSize;
//
//         public OsmTiledDbTileIndexReadOnly(Stream data)
//         {
//             _headerSize = 1;
//             
//             _data = data;
//         }
//         
//         public long? Get(uint tile)
//         {
//             var pointer = Find(tile);
//             if (pointer == null) return null;
//
//             _data.Seek(pointer.Value + 8, SeekOrigin.Begin);
//             return _data.ReadInt64();
//         }
//         
//         private long? Find(uint encoded)
//         {
//             _data.Seek(_headerSize, SeekOrigin.Begin);
//             const int Size = 8 + 4;
//             long start = 0;
//             long end = _data.Length / Size;
//
//             long middle = (end + start) / 2;
//             _data.Seek(middle * Size, SeekOrigin.Begin);
//             var middleId = _data.ReadUInt32();
//             while (middleId != encoded)
//             {
//                 if (middleId > encoded)
//                 {
//                     if (end == middle) return null;
//                     end = middle;
//                 }
//                 else
//                 {
//                     if (start == middle) return null;
//                     start = middle;
//                 }
//                 
//                 middle = (end + start) / 2;
//                 _data.Seek(middle * Size, SeekOrigin.Begin);
//                 middleId = _data.ReadUInt32();
//             }
//
//             return middle * Size;
//         }
//     }
// }