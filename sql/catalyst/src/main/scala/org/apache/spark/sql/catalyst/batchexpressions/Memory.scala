package org.apache.spark.sql.catalyst.batchexpressions

import java.lang.{AssertionError => JAsertError}
import java.nio.ByteOrder

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

  def allocate(width: Int, rowNum: Int): OffHeapMemory = {
    val peer = unsafe.allocateMemory(width * rowNum)
    new OffHeapMemory(peer, rowNum)
  }
}

abstract class Memory {
  def rowNum: Int
  def free(): Unit
}

class OffHeapMemory(val peer: Long, val rowNum: Int) extends Memory {
  import Memory._
  require(peer > 0)

  def free(): Unit = unsafe.freeMemory(peer)
}

class OnHeapMemory(val rowNum: Int) extends Memory {
  //on-heap, do nothing
  override def free() {}
}

/**
 * On-heap version String Array to avoid copy
 */
class StringMemory(val strings: Array[String], rowNum: Int) extends OnHeapMemory(rowNum) {
  def copy(strs: Array[String]) = new StringMemory(strs, this.rowNum)
}

class BooleanMemory(val bs: BitSet, rowNum: Int) extends OnHeapMemory(rowNum) {
  def copy(bitmap: BitSet) = new BooleanMemory(bitmap, this.rowNum)
}

object NullMemory extends OnHeapMemory(0) {
}