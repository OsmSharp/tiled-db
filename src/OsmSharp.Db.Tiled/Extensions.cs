using System;
using System.Collections.Generic;
using OsmSharp.Db.Tiled.Tiles;
using Reminiscence.Arrays;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Contains extension methods.
    /// </summary>
    internal static class Extensions
    {
        /// <summary>
        /// Ensures that this <see cref="ArrayBase{T}"/> has room for at least
        /// the given number of elements, resizing if not.
        /// </summary>
        /// <typeparam name="T">
        /// The type of element stored in this array.
        /// </typeparam>
        /// <param name="array">
        /// This array.
        /// </param>
        /// <param name="minimumSize">
        /// The minimum number of elements that this array must fit.
        /// </param>
        public static void EnsureMinimumSize<T>(this ArrayBase<T> array, long minimumSize)
        {
            if (array.Length < minimumSize)
            {
                IncreaseMinimumSize(array, minimumSize, fillEnd: false, fillValueIfNeeded: default(T));
            }
        }

        /// <summary>
        /// Ensures that this <see cref="ArrayBase{T}"/> has room for at least
        /// the given number of elements, resizing and filling the empty space
        /// with the given value if not.
        /// </summary>
        /// <typeparam name="T">
        /// The type of element stored in this array.
        /// </typeparam>
        /// <param name="array">
        /// This array.
        /// </param>
        /// <param name="minimumSize">
        /// The minimum number of elements that this array must fit.
        /// </param>
        /// <param name="fillValue">
        /// The value to use to fill in the empty spaces if we have to resize.
        /// </param>
        public static void EnsureMinimumSize<T>(this ArrayBase<T> array, long minimumSize, T fillValue)
        {
            if (array.Length < minimumSize)
            {
                IncreaseMinimumSize(array, minimumSize, fillEnd: true, fillValueIfNeeded: fillValue);
            }
        }

        internal static string ToTimestampPath(this DateTime dateTime)
        {
            return dateTime.ToString("yyyy-MM-dd-HH-mm-ss");
        }

        private static void IncreaseMinimumSize<T>(ArrayBase<T> array, long minimumSize, bool fillEnd, T fillValueIfNeeded)
        {
            long oldSize = array.Length;

            // fast-forward, perhaps, through the first several resizes.
            // Math.Max also ensures that we can resize from 0.
            long size = Math.Max(1024, oldSize * 2);
            while (size < minimumSize)
            {
                size *= 2;
            }

            array.Resize(size);
            if (!fillEnd)
            {
                return;
            }

            for (long i = oldSize; i < size; i++)
            {
                array[i] = fillValueIfNeeded;
            }
        }
        
        internal static IEnumerable<OsmGeo> Merge(this IEnumerable<OsmGeo> baseData,
            IEnumerable<OsmGeo> newData)
        {
            return baseData.Merge(newData, (o1, o2) => o1.CompareByIdAndType(o2));
        }

        internal static IEnumerable<Tile> Merge(this IEnumerable<Tile> tiles1, IEnumerable<Tile> tiles2)
        {
            return tiles1.Merge(tiles2, (t1, t2) => t1.LocalId.CompareTo(t2.LocalId));
        }

        internal static IEnumerable<T> Merge<T>(this IEnumerable<T> tiles1, IEnumerable<T> tiles2,
            Func<T, T, int> compare)
        {
            switch (tiles1)
            {
                case null when tiles2 == null:
                    yield break;
                case null:
                {
                    foreach (var tile in tiles2)
                    {
                        yield return tile;
                    }
                    yield break;
                }
            }

            if (tiles2 == null)
            {
                foreach (var tile in tiles1)
                {
                    yield return tile;
                }
                yield break;
            }
            
            using (var baseDataEnumerator = tiles1.GetEnumerator())
            using (var newDataEnumerator = tiles2.GetEnumerator())
            {
                var baseHasNext = baseDataEnumerator.MoveNext();
                var newHasNext = newDataEnumerator.MoveNext();
                
                while (true)
                {
                    // return one of the two.
                    var c = 0;
                    if (baseHasNext && newHasNext)
                    {
                        // data in both, compare.
                        c = compare(baseDataEnumerator.Current, 
                            newDataEnumerator.Current);
                    }
                    else if (baseHasNext)
                    {
                        // on data in base.
                        c = -1;
                    }
                    else if (newHasNext)
                    {
                        // only data in next.
                        c = 1;
                    }
                    else
                    {
                        // no more data!
                        yield break;
                    }

                    if (c == 0)
                    {
                        // oeps, confict.
                        yield return newDataEnumerator.Current;
                        newHasNext = newDataEnumerator.MoveNext();
                        baseHasNext = baseDataEnumerator.MoveNext();
                    }
                    else if (c == -1)
                    {
                        // return from base.
                        yield return baseDataEnumerator.Current;
                        baseHasNext = baseDataEnumerator.MoveNext();
                    }
                    else
                    {
                        // return from new.
                        yield return newDataEnumerator.Current;
                        newHasNext = newDataEnumerator.MoveNext();
                    }
                }
            }
        }
    }
}