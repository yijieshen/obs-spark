package org.apache.spark.sql.catalyst.batchexpressions

import java.lang.{AssertionError => JAsertError}
import java.nio.ByteOrder

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.util.collection.BitSet
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

  //Integral Type
  @inline def setByte(offset: Long, b: Byte): Unit = unsafe.putByte(peer + offset, b)

  @inline def getByte(offset: Long): Byte = unsafe.getByte(peer + offset)

  @inline def setShort(offset: Long, s: Short): Unit = unsafe.putShort(peer + offset, s)

  @inline def getShort(offset: Long): Short = unsafe.getShort(peer + offset)




  @inline def setInt(offset: Long, i: Int): Unit = unsafe.putInt(peer + offset, i)

  @inline def getInt(offset: Long): Int = unsafe.getInt(peer + offset)

  @inline def setLong(offset: Long, l: Long): Unit = unsafe.putLong(peer + offset, l)

  @inline def getLong(offset: Long): Long = unsafe.getLong(peer + offset)

  //Fractional Type
  @inline def setDecimal: Unit = ???

  @inline def getDecimal = ???

  @inline def setFloat(offset: Long, f: Float): Unit = unsafe.putFloat(peer + offset, f)

  @inline def getFloat(offset: Long): Float = unsafe.getFloat(peer + offset)

  @inline def setDouble(offset: Long, d: Double): Unit = unsafe.putDouble(peer + offset, d)

  @inline def getDouble(offset: Long): Double = unsafe.getDouble(peer + offset)

  //Other Type

  //BitMap implementaion?
  def setBit = ???

  //duplicate with bit?
  def setBoolean = ???

  //duplicate with byte?
  def setBinary = ???

  //performance vs long type
  def setTimestamp = ???

  //how do we implement string?
  def setString = ???

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
class StringMemory(strings: Array[String]) extends Memory(0, 0) {
  //on-heap, do nothing
  override def free() {}
}

class BooleanMemory(bs: BitSet) extends Memory(0, 0) {
  //on-heap, do nothing
  override def free() {}
}