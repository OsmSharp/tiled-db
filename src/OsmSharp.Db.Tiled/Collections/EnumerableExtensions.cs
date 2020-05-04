using System;
using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.Collections
{
    internal static class EnumerableExtensions
    {
        public static bool HasOne<T>(this IEnumerable<T> enumerable, out T only)
        {
            only = default!;
            using var enumerator = enumerable.GetEnumerator();
            if (!enumerator.MoveNext()) return false;
            only = enumerator.Current;
            if (enumerator.MoveNext()) return false;
            return true;
        }
        
        public static bool HasCount<T>(this IEnumerable<T> enumerable, int count)
        {
            using var enumerator = enumerable.GetEnumerator();
            while (count > 0)
            {
                if (!enumerator.MoveNext()) return false;
                count--;
            }

            return !enumerator.MoveNext();
        }
        
        public static IEnumerable<T> MergeWhenSorted<T>(this IEnumerable<T> baseEnumerable, IEnumerable<T> recentEnumerable,
            Comparison<T> comparison)
        {
            using var baseEnumerator = baseEnumerable.GetEnumerator();
            using var recentEnumerator = recentEnumerable.GetEnumerator();

            var baseHasNext = baseEnumerator.MoveNext();
            var recentHasNext = recentEnumerator.MoveNext();
            while (baseHasNext || recentHasNext)
            {
                if (baseHasNext && recentHasNext)
                {
                    var baseNext = baseEnumerator.Current;
                    var recentNext = recentEnumerator.Current;
                    var c = comparison(baseNext, recentNext);
                    if (c == 0)
                    {
                        // return most recent.
                        yield return recentNext;
                        baseHasNext = baseEnumerator.MoveNext();
                        recentHasNext = recentEnumerator.MoveNext();
                    }
                    else if (c < 0)
                    {
                        // return base, it's earlier/smaller.
                        yield return baseNext;
                        baseHasNext = baseEnumerator.MoveNext();
                    }
                    else
                    {
                        // return recent, it's earlier/smaller.
                        yield return recentEnumerator.Current;
                        recentHasNext = recentEnumerator.MoveNext();
                    }
                }
                else if (baseHasNext)
                {
                    // only base has data left.
                    yield return baseEnumerator.Current;
                    baseHasNext = baseEnumerator.MoveNext();
                }
                else
                {
                    // only recent has data left.
                    yield return recentEnumerator.Current;
                    recentHasNext = recentEnumerator.MoveNext();
                }
            }
        }
        
        public static IEnumerable<T> MergeWhenSorted<T>(this IEnumerable<T> baseEnumerable, IEnumerable<T> recentEnumerable)
            where T : IComparable<T>
        {
            return baseEnumerable.MergeWhenSorted(recentEnumerable, (a, b) => a.CompareTo(b));
        }
    }
}