using System;
using System.IO;

namespace OsmSharp.Db.Tiled.IO
{
    internal class HugeBufferedStream : Stream
    {
        private readonly Stream _stream;
        private readonly byte[] _buffer;
        private readonly int _blockSize;

        public HugeBufferedStream(Stream stream, int bufferSize = 1024*1024*1024, int blockSize = 1024*1024)
        {
            if (!stream.CanRead) throw new ArgumentException("Stream doesn't support reading.");
            if (!stream.CanSeek) throw new ArgumentException("Stream doesn't support seeking.");
            if (!stream.CanWrite) throw new ArgumentException("Stream doesn't support writing.");
            
            _stream = stream;
            _buffer = new byte[bufferSize];
            _blockSize = blockSize;

            _position = 0;
        }

        private int _bufferPointer = 0;
        private long _bufferPosition = 0;
        private int _bufferSize = 0;
        private long _position;

        private void MoveBuffer()
        {
            if (_position < _bufferPosition)
            { 
                // seek before buffer start, don't move.
                return;
            }
            
            while (_bufferPosition + _bufferSize <= _position)
            { // seek after buffer end.
                // check if buffer can grow.
                if (_bufferSize < _buffer.Length)
                {
                    // the buffer size can still grow.
                    var newSize = (int)(_position - _bufferPointer);
                    if (newSize > _bufferSize)
                    {
                        if (newSize > _buffer.Length) newSize = _buffer.Length;

                        _bufferSize = newSize;
                    }
                }
                else
                {
                    // user has move beyond the buffer and it can't grow anymore.
                    // flush first part of the buffer.
                    _stream.Seek(_bufferPosition, SeekOrigin.Begin);
                    var bytesToFlush = _blockSize;
                    if (_bufferPointer + bytesToFlush > _buffer.Length)
                    {
                        // part to flush overlaps end of stream.
                        _stream.Write(_buffer, _bufferPointer,  _buffer.Length - bytesToFlush);
                        bytesToFlush -= (_buffer.Length - bytesToFlush);
                        _bufferPosition += (_buffer.Length - bytesToFlush);
                        _bufferPointer = 0;
                    }
                    if (bytesToFlush > 0)
                    {
                        _stream.Write(_buffer, _bufferPointer, bytesToFlush);
                    }
                    _bufferPosition += bytesToFlush;
                    _bufferPointer += bytesToFlush;
                    if (_bufferPointer >= _bufferSize) _bufferPointer = _bufferPointer - _bufferSize;
                }
            }
        }

        private void GrowBuffer(int offset)
        {
            var position = _position + offset;
            if (position < _bufferPosition)
            { 
                // all data is before buffer, don't grow it.
                return;
            }

            var size = (int)(position - _bufferPosition);
            if (_bufferSize > size)
            {
                // no need to grow.
                return;
            }

            if (size > _buffer.Length)
            {
                // grown until max.
                size = _buffer.Length;
            }

            // resize buffer and make sure position is optimal.
            _bufferSize = size;
            MoveBuffer();
        }
        
        public override void Flush()
        {
            _stream.Seek(_bufferPosition, SeekOrigin.Begin);
            if (_bufferSize < _buffer.Length)
            {
                _stream.Write(_buffer, _bufferPointer, _bufferSize);
            }
            else
            {
                _stream.Write(_buffer, _bufferPointer, _buffer.Length - _bufferPointer);
                if (_bufferPointer > 0)
                {
                    _stream.Write(_buffer, 0, _bufferPointer);
                }
            }
            
            _stream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_position + count < _bufferPosition)
            {
                // completely before buffer.
                _stream.Seek(_position, SeekOrigin.Begin);
                return _stream.Read(buffer, offset, count);
            }
            else
            {
                // read bytes before buffer if any.
                var bytesBeforeBuffer = (int)(_bufferPosition - _position);
                if (bytesBeforeBuffer > 0)
                {
                    _stream.Seek(_position, SeekOrigin.Begin);
                    _stream.Read(buffer, offset, bytesBeforeBuffer);
                    count -= bytesBeforeBuffer;
                    offset += bytesBeforeBuffer;
                }
                
                // read bytes from buffer at end if there is an overlap.
                var bufferPointer = _bufferPointer;
                var bufferSpan = buffer.AsSpan(offset);
                if (bufferPointer + count > _bufferSize)
                {
                    // read overlaps buffer end.
                    var bytesAtEnd = _bufferSize - bufferPointer;
                    _buffer.AsSpan(bufferPointer, bytesAtEnd).CopyTo(bufferSpan);
                    offset += bytesAtEnd;
                    count -= bytesAtEnd;
                    bufferPointer = 0;
                    bufferSpan = buffer.AsSpan(offset);
                }
                
                // read bytes from buffer.
                _buffer.AsSpan(bufferPointer, count).CopyTo(bufferSpan);
                return count;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (origin == SeekOrigin.Begin)
            {
                _position = offset;
            }
            else if (origin == SeekOrigin.Current)
            {
                _position += offset;
            }
            else
            {
                _position = this.Length - offset;
            }
            
            MoveBuffer();

            return _position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (_position + count < _bufferPosition)
            {
                // completely before buffer.
                _stream.Seek(_position, SeekOrigin.Begin);
                _stream.Write(buffer, offset, count);
                _position = _stream.Position;
            }
            else
            {
                var originalCount = count;
                
                // grow buffer if needed.
                GrowBuffer(count);
                
                // write bytes before buffer if any.
                var bytesBeforeBuffer = (int)(_bufferPosition - _position);
                if (bytesBeforeBuffer > 0)
                {
                    _stream.Seek(_position, SeekOrigin.Begin);
                    _stream.Write(buffer, offset, bytesBeforeBuffer);
                    count -= bytesBeforeBuffer;
                    offset += bytesBeforeBuffer;
                }
                
                // writes bytes to buffer at end if there is an overlap.
                var bufferPointer = _bufferPointer;
                var bufferOffset = (int) (_position - _bufferPosition);
                var bufferSpan = _buffer.AsSpan((bufferOffset + _bufferPointer) % _bufferSize, _bufferSize - bufferOffset);
                if (count > bufferSpan.Length)
                {
                    // write overlaps buffer end.
                    var bytesAtEnd = _bufferSize - bufferPointer;
                    buffer.AsSpan(offset, bytesAtEnd).CopyTo(bufferSpan);
                    offset += bytesAtEnd;
                    count -= bytesAtEnd;
                    bufferPointer = 0;
                    bufferSpan = buffer.AsSpan(bufferPointer);
                }
                
                // write bytes to buffer.
                buffer.AsSpan(offset, count).CopyTo(bufferSpan);

                _position += originalCount;
            }
        }

        public override bool CanRead { get; } = true;
        public override bool CanSeek { get; } = true;
        public override bool CanWrite { get; } = true;

        public override long Length
        {
            get
            {
                var bufferEnd = _bufferPosition + _bufferSize;
                if (bufferEnd > _stream.Length) return bufferEnd;
                return _stream.Length;
            }
        }

        public override long Position
        {
            get => _position;
            set => this.Seek(value, SeekOrigin.Begin);
        }
    }
}