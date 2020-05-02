using System;

namespace OsmSharp.Db.Tiled.Logging
{
    internal class ProgressAbsolute
    {
        private readonly Log _log;
        private readonly TraceEventType _type;
        private readonly Func<long, string>? _getMessage = null;
        private readonly int _incrementSize;
        
        public ProgressAbsolute(Log log, TraceEventType type = TraceEventType.Verbose,
            Func<long, string>? getMessage = null, int incrementSize = 1000)
        {
            _log = log;
            _type = type;
            _getMessage = getMessage;
            _incrementSize = incrementSize;
        }

        private long _lastReported = 0;
        
        public void Progress(long i)
        {
            if (_incrementSize > 0) i = (long)System.Math.Floor(i / (double)_incrementSize);
            
            if (_lastReported >= i) return;
            
            var message = _getMessage?.Invoke(i * _incrementSize) ?? $"Processed {i}";
            _lastReported = i;
            _log.Message(_type, message);
        }
    }
}