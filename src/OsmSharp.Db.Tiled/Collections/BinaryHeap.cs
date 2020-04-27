using System;

namespace OsmSharp.Db.Tiled.Collections
{
 /// <summary>
    /// Implements a priority queue in the form of a binary heap.
    /// </summary>
    internal class BinaryHeap
    {
        private long[] _priorities; // Holds the priorities of this heap.
        private int _count; // The current count of elements.
        private uint _latestIndex; // The latest unused index

        /// <summary>
        /// Creates a new binary heap.
        /// </summary>
        public BinaryHeap()
            : this(2)
        {

        }

        /// <summary>
        /// Creates a new binary heap.
        /// </summary>
        public BinaryHeap(uint initialSize)
        {
            _priorities = new long[initialSize];

            _count = 0;
            _latestIndex = 1;
        }

        /// <summary>
        /// Returns the number of items in this queue.
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Enqueues a given item.
        /// </summary>
        public void Push(long priority)
        {
            _count++; // another item was added!

            // increase size if needed.
            if (_latestIndex == _priorities.Length - 1)
            {
                // time to increase size!
                Array.Resize(ref _priorities, _priorities.Length + 100);
            }

            // add the item at the first free point 
            _priorities[_latestIndex] = priority;

            // ... and let it 'bubble' up.
            var bubbleIndex = _latestIndex;
            _latestIndex++;
            while (bubbleIndex != 1)
            {
                // bubble until the index is one.
                var parentIdx = bubbleIndex / 2;
                if (_priorities[bubbleIndex] < _priorities[parentIdx])
                {
                    // the parent priority is higher; do the swap.
                    var tempPriority = _priorities[parentIdx];
                    _priorities[parentIdx] = _priorities[bubbleIndex];
                    _priorities[bubbleIndex] = tempPriority;

                    bubbleIndex = parentIdx;
                }
                else
                {
                    // the parent priority is lower or equal; the item will not bubble up more.
                    break;
                }
            }
        }

        /// <summary>
        /// Returns the smallest weight in the queue.
        /// </summary>
        public double PeekWeight()
        {
            return _priorities[1];
        }

        /// <summary>
        /// Returns the object with the smallest weight and removes it.
        /// </summary>
        public long Pop()
        {
            var priority = 0L;
            if (_count <= 0) return long.MinValue;

            priority = _priorities[1];

            _count--; // reduce the element count.
            _latestIndex--; // reduce the latest index.

            var swapItem = 1;
            var parentPriority = _priorities[_latestIndex];
            _priorities[1] = parentPriority; // place the last element on top.
            do
            {
                var parent = swapItem;
                var swapItemPriority = 0L;
                if ((2 * parent + 1) <= _latestIndex)
                {
                    swapItemPriority = _priorities[2 * parent];
                    var potentialSwapItem = _priorities[2 * parent + 1];
                    if (parentPriority >= swapItemPriority)
                    {
                        swapItem = 2 * parent;
                        if (_priorities[swapItem] >= potentialSwapItem)
                        {
                            swapItemPriority = potentialSwapItem;
                            swapItem = 2 * parent + 1;
                        }
                    }
                    else if (parentPriority >= potentialSwapItem)
                    {
                        swapItemPriority = potentialSwapItem;
                        swapItem = 2 * parent + 1;
                    }
                    else
                    {
                        break;
                    }
                }
                else if ((2 * parent) <= _latestIndex)
                {
                    // Only one child exists
                    swapItemPriority = _priorities[2 * parent];
                    if (parentPriority >= swapItemPriority)
                    {
                        swapItem = 2 * parent;
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    break;
                }

                _priorities[parent] = swapItemPriority;
                _priorities[swapItem] = parentPriority;

            } while (true);

            return priority;
        }

        /// <summary>
        /// Clears this priority queue.
        /// </summary>
        public void Clear()
        {
            _count = 0;
            _latestIndex = 1;
        }
    }
}