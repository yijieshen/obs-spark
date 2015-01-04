package org.apache.spark.sql.catalyst.batchexpressions

import java.util.Arrays

class ByteArrayMap(initialCapacity: Int = 64)
  extends Iterable[(Array[Byte], Array[Byte])] with Serializable {
  require(initialCapacity <= (1 << 29), "Can't make capacity bigger than 2^29 elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  private val LOAD_FACTOR = 0.7

  private var capacity = nextPowerOf2(initialCapacity)
  private var mask = capacity - 1
  private var curSize = 0
  private var growThreshold = (LOAD_FACTOR * capacity).toInt

  private var keys = new Array[Array[Byte]](capacity)
  private var values = new Array[Array[Byte]](capacity)

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  def get(key: Array[Byte]): Array[Byte] = {
    var pos = key.hashCode() & mask //TODO: hash code or MurmurHash?
    var i = 1
    while (true) {
      val curKey = keys(pos)
      if(key.eq(curKey) || Arrays.equals(key, curKey)) {
        return values(pos)
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[Array[Byte]]
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[Array[Byte]]
  }

  /**
   * If a match is find, return its current vale, return a new array otherwise
   * @param bufferLength if a key doesn't exist, the length of Array[Byte] to create
   */
  def getValue(key: Array[Byte], bufferLength: Int): Array[Byte] = {
    var pos = key.hashCode() & mask //TODO: hash code or MurmurHash?
    var i = 1
    while (true) {
      val curKey = keys(pos)
      if(key.eq(curKey) || Arrays.equals(key, curKey)) {
        return values(pos)
      } else if (curKey.eq(null)) {
        keys(pos) = key
        values(pos) = new Array[Byte](bufferLength)
        return values(pos)
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[Array[Byte]]
  }

  def update(key: Array[Byte], value: Array[Byte]): Unit = {
    if(key.eq(null)) return
    var pos = key.hashCode() & mask
    var i = 1
    while (true) {
      val curKey = keys(pos)
      if(curKey.eq(null)) {
        keys(pos) = key
        values(pos) = value
        incrementSize()
        return
      } else if (key.eq(curKey) || Arrays.equals(key, curKey)) {
        values(pos) = value
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /** Increase table size by 1, rehashing if necessary */
  private def incrementSize() {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  protected def growTable() = {
    val newCapacity = capacity * 2
    if (newCapacity >= (1 << 30)) {
      // We can't make the table this big because we want an array of 2x
      // that size for our data, but array sizes are at most Int.MaxValue
      throw new Exception("Can't make capacity bigger than 2^29 elements")
    }
    val newKeys = new Array[Array[Byte]](2 * newCapacity)
    val newValues = new Array[Array[Byte]](2 * newCapacity)
    val newPartitions = new Array[Int](2 * newCapacity)
    val newMask = newCapacity - 1
    var oldPos = 0
    while (oldPos < capacity) {
      if (!keys(oldPos).eq(null)) {
        val key = keys(oldPos)
        val value = values(oldPos)
        var newPos = key.hashCode & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newKeys(newPos)
          if (curKey.eq(null)) {
            newKeys(newPos) = key
            newValues(newPos) = value
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    keys = newKeys
    values = newValues
    capacity = newCapacity
    mask = newMask
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  override def iterator: Iterator[(Array[Byte], Array[Byte])] = {
    new Iterator[(Array[Byte], Array[Byte])] {
      var pos = 0

      def nextValue(): (Array[Byte], Array[Byte]) = {
        while (pos < capacity) {
          if(!keys(pos).eq(null)) {
            return (keys(pos), values(pos))
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): (Array[Byte], Array[Byte]) = {
        val kv = nextValue()
        pos += 1
        kv
      }
    }
  }


}
