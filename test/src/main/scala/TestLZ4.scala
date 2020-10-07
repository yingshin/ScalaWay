import net.jpountz.lz4.LZ4Factory
import org.apache.flink.runtime.io.compression.Lz4BlockCompressionFactory

/**
 * Author: zhangying14
 * Date: 2020/9/16 14:02
 * Package: 
 * Description:
 *
 */
object TestLZ4 extends App {
  val factory = new Lz4BlockCompressionFactory()

  val compressor = factory.getCompressor
  val decompressor = factory.getDecompressor

  val data = (1600236299 to 1600236299 + 1000).mkString(",")
  val dataLen = data.length
  val maxCompressedLen = compressor.getMaxCompressedSize(data.length)

  val compressedBuffer = new Array[Byte](maxCompressedLen)
  val compressedDataLen = compressor.compress(data.getBytes, 0, data.length, compressedBuffer, 0)
  println(dataLen, maxCompressedLen, compressedDataLen)

  val deCompressedBuffer = new Array[Byte](dataLen)
  val deCompressedDataLen = decompressor.decompress(compressedBuffer, 0, compressedDataLen, deCompressedBuffer, 0)

  println(new String(deCompressedBuffer, 0, deCompressedDataLen))
}
