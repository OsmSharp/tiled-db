using System;

namespace OsmSharp.Db.Tiled.Logging
{
    internal class ProgressRelative
    {
        private readonly Log _log;
        private readonly TraceEventType _type;
        private readonly Func<int, string>? _getMessage = null;
        private readonly int _increments = 10;
        private readonly int _minimum = 1000;
        
        public ProgressRelative(Log log, TraceEventType type = TraceEventType.Verbose,
            Func<int, string>? getMessage = null)
        {
            _log = log;
            _type = type;
            _getMessage = getMessage;
        }

        private int _lastReported = -1;

        public void Progress(int i, int total)
        {
            if (total < _minimum) return;
            Progress(i/(double)total);
        }
        
        public void Progress(double progress)
        {
            var percentage = (int)(progress * 100);
            if (percentage < 0) percentage = 0;
            if (percentage > 100) percentage = 100;

            if (_increments > 0) percentage /= _increments;

            if (_lastReported >= percentage) return;
            
            var message = _getMessage?.Invoke(percentage * _increments) ?? $"Processed {percentage}%";
            _lastReported = percentage;
            _log.Message(_type, message);
        }

        public void Done()
        {
            Progress(1);
        }
    }
}