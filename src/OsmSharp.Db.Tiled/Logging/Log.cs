using System;
using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.Logging
{
    /// <summary>
    /// A logger (to prevent a dependency on something like serilog).
    /// </summary>
    public class Log
    {
        private static readonly Lazy<Log> DefaultLog = new Lazy<Log>(() => new Log());

        internal Log()
        {
            
        }

        /// <summary>
        /// Gets the default logger.
        /// </summary>
        internal static Log Default => DefaultLog.Value;

        internal void Message(TraceEventType type, string message)
        {
            LogAction?.Invoke(type, message);
        }

        internal void Information(string message)
        {
            LogAction?.Invoke(TraceEventType.Information, message);
        }

        internal void Debug(string message)
        {
            LogAction?.Invoke(TraceEventType.Debug, message);
        }

        internal void Critical(string message)
        {
            LogAction?.Invoke(TraceEventType.Critical, message);
        }

        internal void Warning(string message)
        {
            LogAction?.Invoke(TraceEventType.Warning, message);
        }

        internal void Error(string message)
        {
            LogAction?.Invoke(TraceEventType.Error, message);
        }

        internal void Verbose(string message)
        {
            LogAction?.Invoke(TraceEventType.Verbose, message);
        }
        
        /// <summary>
        /// Defines the log action function.
        /// </summary>
        /// <param name="type">The message type.</param>
        /// <param name="message">The message content.</param>
        public delegate void LogActionFunction(TraceEventType type, string message);

        /// <summary>
        /// Gets or sets the action to actually log a message.
        /// </summary>
        public static LogActionFunction? LogAction
        {
            get;
            set;
        }
    }
}