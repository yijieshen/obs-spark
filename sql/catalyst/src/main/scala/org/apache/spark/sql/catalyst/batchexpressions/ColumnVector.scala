package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.types._
import Memory._

abstract class ColumnVector(val isTemp: Boolean = false) {

  def dt: DataType
  def typeWidth: Int

  type fieldType <: Any

  def content: Memory

  //(null -> 0) (notNull -> 1)
  var notNullArray: BitSet = _

  def get(i: Int): fieldType
  def set(i: Int, v: fieldType)

  def setNullable(i: Int, nullable: Any) = {
    if(nullable == null) {
      if(notNullArray == null) {
        notNullArray = new BitSet(content.rowNum)
      }
      notNullArray.set(i)
    } else {
      set(i, nullable.asInstanceOf[fieldType])
    }
  }

  def reinit = notNullArray = null

  def extractTo(row: MutableRow, ordinal: Int, index: Int) = {
    if(notNullArray == null || notNullArray.get(index)) {
      row.setNullAt(ordinal)
    } else {
      setField(row, ordinal, index)
    }
  }

  def setField(row: MutableRow, ordinal: Int, index: Int)

}

class DoubleColumnVector(val content: OffHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = DoubleType.JvmType
  val dt = DoubleType
  val typeWidth = 8
  lazy val peer = content.peer
  def get(i: Int) = unsafe.getDouble(peer + i * typeWidth)
  def set(i: Int, v: fieldType) = unsafe.putDouble(peer + i * typeWidth, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setDouble(ordinal, get(index))
  }
}

class LongColumnVector(val content: OffHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = LongType.JvmType
  val dt = LongType
  val typeWidth = 8
  lazy val peer = content.peer
  def get(i: Int) = unsafe.getLong(peer + i * typeWidth)
  def set(i: Int, v: fieldType) = unsafe.putLong(peer + i * typeWidth, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setLong(ordinal, get(index))
  }
}

class IntColumnVector(val content: OffHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = IntegerType.JvmType
  val dt = IntegerType
  val typeWidth = 4
  lazy val peer = content.peer
  def get(i: Int) = unsafe.getInt(peer + i * typeWidth)
  def set(i: Int, v: fieldType) = unsafe.putInt(peer + i * typeWidth, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setInt(ordinal, get(index))
  }
}

class FloatColumnVector(val content: OffHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = FloatType.JvmType
  val dt = FloatType
  val typeWidth = 4
  lazy val peer = content.peer
  def get(i: Int) = unsafe.getFloat(peer + i * typeWidth)
  def set(i: Int, v: fieldType) = unsafe.putFloat(peer + i * typeWidth, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setFloat(ordinal, get(index))
  }
}

class ShortColumnVector(val content: OffHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = ShortType.JvmType
  val dt = ShortType
  val typeWidth = 2
  lazy val peer = content.peer
  def get(i: Int) = unsafe.getShort(peer + i * typeWidth)
  def set(i: Int, v: fieldType) = unsafe.putShort(peer + i * typeWidth, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setShort(ordinal, get(index))
  }
}

class ByteColumnVector(val content: OffHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = ByteType.JvmType
  val dt = ByteType
  val typeWidth = 1
  val peer = content.peer
  def get(i: Int) = unsafe.getByte(peer + i * typeWidth)
  def set(i: Int, v: fieldType) = unsafe.putByte(peer + i * typeWidth, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setByte(ordinal, get(index))
  }
}

class StringColumnVector(val content: OnHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = String
  val dt = StringType
  val typeWidth = 0
  val strings = content.asInstanceOf[StringMemory].strings
  def get(i: Int) = strings(i)
  def set(i: Int, v: fieldType) = strings(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setString(ordinal, get(index))
  }
}

class BinaryColumnVector(val content: OnHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = Array[Byte]
  val dt = BinaryType
  val typeWidth = 0
  def get(i: Int) = ???
  def set(i: Int, v: fieldType) = ???

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.update(ordinal, get(index))
  }
}

class BooleanColumnVector(val content: OnHeapMemory, isTemp: Boolean) extends ColumnVector(isTemp) {
  type fieldType = Boolean
  val dt = BooleanType
  val typeWidth = 0
  val bitset = content.asInstanceOf[BooleanMemory].bs
  def get(i: Int) = bitset.get(i)
  def set(i: Int, v: fieldType) = bitset.set(i, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setBoolean(ordinal, get(index))
  }
}

abstract class FakeColumnVector extends ColumnVector(false) {
  val content = NullMemory
}

class DoubleLiteral(val value: Double) extends FakeColumnVector {
  type fieldType = DoubleType.JvmType
  val dt = DoubleType
  val typeWidth = 8
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal Double cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setDouble(ordinal, value)
  }
}

class LongLiteral(val value: Long) extends FakeColumnVector {
  type fieldType = LongType.JvmType
  val dt = LongType
  val typeWidth = 8
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal Long cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setLong(ordinal, value)
  }
}

class IntLiteral(val value: Int) extends FakeColumnVector {
  type fieldType = IntegerType.JvmType
  val dt = IntegerType
  val typeWidth = 4
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal Int cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setInt(ordinal, value)
  }
}

class FloatLiteral(val value: Float) extends FakeColumnVector {
  type fieldType = FloatType.JvmType
  val dt = FloatType
  val typeWidth = 4
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal Float cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setFloat(ordinal, value)
  }
}

class ShortLiteral(val value: Short) extends FakeColumnVector {
  type fieldType = ShortType.JvmType
  val dt = ShortType
  val typeWidth = 2
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal Short cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setShort(ordinal, value)
  }
}

class ByteLiteral(val value: Byte) extends FakeColumnVector {
  type fieldType = ByteType.JvmType
  val dt = ByteType
  val typeWidth = 1
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal Byte cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setByte(ordinal, value)
  }
}

class StringLiteral(val value: String) extends FakeColumnVector {
  type fieldType = String
  val dt = StringType
  val typeWidth = 0
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal String cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setString(ordinal, value)
  }
}

class BinaryLiteral(val value: Array[Byte]) extends FakeColumnVector {
  type fieldType = Array[Byte]
  val dt = BinaryType
  val typeWidth = 0
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal Binary cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.update(ordinal, value)
  }
}

class BooleanLiteral(val value: Boolean) extends FakeColumnVector {
  type fieldType = Boolean
  val dt = BooleanType
  val typeWidth = 0
  def get(i: Int) = value
  def set(i: Int, v: fieldType) = sys.error(s"Literal Boolean cannot set")

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setBoolean(ordinal, value)
  }
}

object ColumnVector {
  def getNewCV(dt: DataType, content: Memory, isTmp: Boolean) = {
    dt match {
      case DoubleType => new DoubleColumnVector(content.asInstanceOf[OffHeapMemory], isTmp)
      case LongType => new LongColumnVector(content.asInstanceOf[OffHeapMemory], isTmp)
      case IntegerType => new IntColumnVector(content.asInstanceOf[OffHeapMemory], isTmp)
      case FloatType => new FloatColumnVector(content.asInstanceOf[OffHeapMemory], isTmp)
      case ShortType => new ShortColumnVector(content.asInstanceOf[OffHeapMemory], isTmp)
      case ByteType => new ByteColumnVector(content.asInstanceOf[OffHeapMemory], isTmp)
      case StringType => new StringColumnVector(content.asInstanceOf[OnHeapMemory], isTmp)
      case BinaryType => new BinaryColumnVector(content.asInstanceOf[OnHeapMemory], isTmp)
      case BooleanType => new BooleanColumnVector(content.asInstanceOf[OnHeapMemory], isTmp)
    }
  }

  def getLiteral(value: Any, dt: DataType) = {
    dt match {
      case DoubleType => new DoubleLiteral(value.asInstanceOf[Double])
      case LongType => new LongLiteral(value.asInstanceOf[Long])
      case IntegerType => new IntLiteral(value.asInstanceOf[Int])
      case FloatType => new FloatLiteral(value.asInstanceOf[Float])
      case ShortType => new ShortLiteral(value.asInstanceOf[Short])
      case ByteType => new ByteLiteral(value.asInstanceOf[Byte])
      case StringType => new StringLiteral(value.asInstanceOf[String])
      case BinaryType => new BinaryLiteral(value.asInstanceOf[Array[Byte]])
      case BooleanType => new BooleanLiteral(value.asInstanceOf[Boolean])
    }
  }
}