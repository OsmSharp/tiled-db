using System;
using System.IO;
using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;

namespace OsmSharp.Db.Tiled.Tests.IO
{
    [TestFixture]
    public class HugeBufferedStreamTests
    {
        [Test]
        public void HugeBufferedStream_WriteByte_ShouldWriteByte()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);
            bufferedStream.WriteByte(129);
            bufferedStream.Flush();
            
            Assert.AreEqual(1, memoryStream.Length);
            memoryStream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(129, memoryStream.ReadByte());
        }
        
        [Test]
        public void HugeBufferedStream_WriteByte_ShouldMovePosition()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);
            bufferedStream.WriteByte(129);
            
            Assert.AreEqual(1, bufferedStream.Position);
        }
        
        [Test]
        public void HugeBufferedStream_WriteByte_ShouldUpdateLength()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);
            bufferedStream.WriteByte(129);
            
            Assert.AreEqual(1, bufferedStream.Length);
        }
        
        [Test]
        public void HugeBufferedStream_WriteWithinBuffer_ShouldWriteBytes()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);

            for (var i = 0; i < 128; i++)
            {
                bufferedStream.WriteByte((byte)i);
            }
            bufferedStream.Flush();

            for (var i = 0; i < 128; i++)
            {
                memoryStream.Seek(i, SeekOrigin.Begin);
                Assert.AreEqual((byte)i, memoryStream.ReadByte());
            }
        }
        
        [Test]
        public void HugeBufferedStream_Seek_WithingBuffer_ShouldMoveToBufferedBytes()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);

            for (var i = 0; i < 128; i++)
            {
                bufferedStream.WriteByte((byte)i);
            }

            bufferedStream.Seek(65, SeekOrigin.Begin);
            Assert.AreEqual(65, bufferedStream.ReadByte());

            bufferedStream.Seek(1, SeekOrigin.Begin);
            Assert.AreEqual(1, bufferedStream.ReadByte());

            bufferedStream.Seek(127,SeekOrigin.Begin);
            Assert.AreEqual(127, bufferedStream.ReadByte());
        }
        
        [Test]
        public void HugeBufferedStream_Seek_BeforeBuffer_ShouldMoveToBaseStreamBytes()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);

            for (var i = 0; i < 256; i++)
            {
                bufferedStream.WriteByte((byte)i);
            }

            bufferedStream.Seek(65, SeekOrigin.Begin);
            Assert.AreEqual(65, bufferedStream.ReadByte());

            bufferedStream.Seek(1, SeekOrigin.Begin);
            Assert.AreEqual(1, bufferedStream.ReadByte());

            bufferedStream.Seek(127,SeekOrigin.Begin);
            Assert.AreEqual(127, bufferedStream.ReadByte());
        }
        
        [Test]
        public void HugeBufferedStream_WriteByte_BeyondBuffer_ShouldWriteBytes()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);

            for (var i = 0; i < 1024; i++)
            {
                bufferedStream.WriteByte((byte)(i % 256));
            }
            bufferedStream.Flush();

            for (var i = 0; i < 1024; i++)
            {
                memoryStream.Seek(i, SeekOrigin.Begin);
                Assert.AreEqual((byte)(i % 256), memoryStream.ReadByte(), $"Data at {i} doesn't match.");
            }
        }
        
        [Test]
        public void HugeBufferedStream_Write_BeyondBuffer_ShouldWriteBytes()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);

            var data = new byte[128];
            for (var i = 0; i < 128; i++)
            {
                bufferedStream.WriteByte((byte)(byte.MaxValue - (i % 256)));
                data[i] = (byte) (i % 256);
            }
            bufferedStream.Write(data, 0, data.Length);
            bufferedStream.Flush();

            for (var i = 0; i < 128; i++)
            {
                memoryStream.Seek(i + 128, SeekOrigin.Begin);
                Assert.AreEqual((byte)(i % 256), memoryStream.ReadByte(), $"Data at {i} doesn't match.");
            }
        }
        
        [Test]
        public void HugeBufferedStream_Write_BeyondBuffer_NotAligned_ShouldWriteBytes()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);

            for (var i = 0; i < 64; i++)
            {
                bufferedStream.WriteByte((byte)(byte.MaxValue - (i % 256)));
            }
            
            var data = new byte[128];
            for (var i = 0; i < 128; i++)
            {
                data[i] = (byte) (i % 256);
            }
            bufferedStream.Write(data, 0, data.Length);
            bufferedStream.Flush();

            for (var i = 0; i < 128; i++)
            {
                memoryStream.Seek(i + 64, SeekOrigin.Begin);
                Assert.AreEqual((byte)(i % 256), memoryStream.ReadByte(), $"Data at {i} doesn't match.");
            }
        }
        
        [Test]
        public void HugeBufferedStream_WriteByte_BeforeBuffer_ShouldWriteToBaseStream()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);

            for (var i = 0; i < 1024; i++)
            {
                bufferedStream.WriteByte((byte)(i % 256));
            }

            bufferedStream.Seek(0, SeekOrigin.Begin);
            for (var i = 0; i < 128; i++)
            {
                bufferedStream.WriteByte((byte)(265 - (i % 256)));
            }
            bufferedStream.Flush();

            for (var i = 0; i < 128; i++)
            {
                memoryStream.Seek(i, SeekOrigin.Begin);
                Assert.AreEqual((byte)(265 - (i % 256)), memoryStream.ReadByte(), $"Data at {i} doesn't match.");
            }
        }
        
        [Test]
        public void HugeBufferedStream_WriteByte_BeforeAndOnBuffer_ShouldWriteTo()
        {
            var memoryStream = new MemoryStream();
            var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);

            for (var i = 0; i < 1024; i++)
            {
                bufferedStream.WriteByte((byte)(i % 256));
            }

            // overwrite data again
            bufferedStream.Seek(0, SeekOrigin.Begin);
            for (var i = 0; i < 1024; i++)
            {
                bufferedStream.WriteByte((byte)(265 - (i % 256)));
            }
            bufferedStream.Flush();

            for (var i = 0; i < 1024; i++)
            {
                memoryStream.Seek(i, SeekOrigin.Begin);
                Assert.AreEqual((byte)(265 - (i % 256)), memoryStream.ReadByte(), $"Data at {i} doesn't match.");
            }
        }
        
        
        //
        // [Test]
        // public void HugeBufferStream_WhileWriting_ReadingShouldBeConsistent()
        // {
        //     var memoryStream = new MemoryStream();
        //     var bufferedStream = new HugeBufferedStream(memoryStream, 128, 32);
        //
        //     for (var i = 0; i < 1024; i++)
        //     {
        //         bufferedStream.WriteByte((byte)(i % 256));
        //         
        //     }
        //     bufferedStream.Flush();
        //
        //     for (var i = 0; i < 1024; i++)
        //     {
        //         memoryStream.Seek(i, SeekOrigin.Begin);
        //         Assert.AreEqual((byte)(i % 256), memoryStream.ReadByte(), $"Data at {i} doesn't match.");
        //     }
        // }
    }
}