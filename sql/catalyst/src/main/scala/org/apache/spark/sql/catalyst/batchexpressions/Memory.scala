package org.apache.spark.sql.catalyst.batchexpressions

import java.lang.{AssertionError => JAsertError}
import java.nio.ByteOrder

import org.apache.spark.sql.catalyst.types._
import sun.misc.Unsafe

/**
 * Part adapted from org.apache.cassandra.io.util.Memory version 2.0.7
 */
object Memory {
  val unsafe = try {
    val field = classOf[Unsafe].getDeclaredField("theUnsafe")
    field.setAccessible(true)
    field.get(null).asInstanceOf[Unsafe]
  } catch {
    case ex: Throwable => throw new JAsertError(ex)
  }

  //When would we need this?
  lazy val bigEndian: Boolean = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)
  lazy val unaligned: Boolean = {
    val arch = System.getProperty("os.arch")
    (arch == "i386") || (arch == "x86") || (arch == "amd64") || (arch == "x86_64")
  }

  def allocate(bytes: Long): Memory = {
    require(bytes > 0)
    val peer = unsafe.allocateMemory(bytes)
    new Memory(peer, bytes)
  }

  val setValue =
    (dt: DataType) =>
      dt match {
        case DoubleType => unsafe.putDouble( _ : Long, _ : Double)
        case LongType => unsafe.putLong( _ : Long, _ : Long)
        case IntegerType => unsafe.putInt( _ : Long, _ : Int)
        case FloatType => unsafe.putFloat( _ : Long, _ : Float)
        case ShortType => unsafe.putShort( _ : Long, _ : Short)
        case ByteType => unsafe.putByte( _ : Long, _ : Byte)
        case _ => throw new
            UnsupportedOperationException(s"Type $dt does not support unsafe")
      }

  val getValue =
    (dt: DataType) =>
      dt match {
        case DoubleType => unsafe.getDouble(_ : Long)
        case LongType => unsafe.getLong(_ : Long)
        case IntegerType => unsafe.getInt(_ : Long)
        case FloatType => unsafe.getFloat(_ : Long)
        case ShortType => unsafe.getShort(_ : Long)
        case ByteType => unsafe.getByte(_ : Long)
        case _ => throw new
            UnsupportedOperationException(s"Type $dt does not support unsafe")
      }
}

/**
 *
 * Note: Current getter & setters are implemented without position guards,
 * user of this class should take care of this
 * @param peer Start address of allocated Memory
 * @param size Size of memory asked for
 */
class Memory(val peer: Long, val size: Long) {
  require(peer > 0)

  import Memory._

  def free(): Unit = unsafe.freeMemory(peer)

  //TODO: Do we need this guard? Currently, this method was never used
  @inline
  def checkPosition(offset: Long): Unit = assert(offset >= 0 && offset < size)

  override def equals(other: Any): Boolean =
    other match {
      case that: Memory => peer == that.peer && size == that.size
      case _ => false
    }

  override def hashCode: Int = peer.hashCode() * 31 + size.hashCode()

}

/**
 * On-heap version String Array to avoid copy
 * @param strings
 */
class StringMemory(val strings: Array[String]) extends Memory(0, 0) {
  //on-heap, do nothing
  override def free() {}
}

class BooleanMemory(val bs: BitSet) extends Memory(0, 0) {
  //on-heap, do nothing
  override def free() {}
}

object NullMemory extends Memory(0, 0) {
  override def free() {}
}