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
        public void HugeBufferStream_WriteWithinBuffer_ShouldWriteBytes()
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
        public void HugeBufferStream_WriteBeyondBuffer_ShouldWriteBytes()
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
    }
}