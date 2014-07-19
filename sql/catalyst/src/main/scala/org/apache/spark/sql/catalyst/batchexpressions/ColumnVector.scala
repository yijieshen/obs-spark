package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.types._

abstract class ColumnVector {

  def rowNum: Int
  type fieldType <: Any
  def content: Array[fieldType]

  //(null -> 0) (notNull -> 1)
  var notNullArray: BitSet = _

  def get(i: Int) = content(i)
  def set(i: Int, v: fieldType) = content(i) = v

  def setNullable(i: Int, nullable: Any) = {
    if(nullable == null) {
      if(notNullArray == null) {
        notNullArray = (new BitSet(rowNum)).complement
      }
      notNullArray.set(i, false)
    } else {
      set(i, nullable.asInstanceOf[fieldType])
    }
  }

  def reinit = notNullArray = null

  def extractTo(row: MutableRow, ordinal: Int, index: Int) = {
    if(notNullArray != null && !notNullArray.get(index)) {
      row.setNullAt(ordinal)
    } else {
      setField(row, ordinal, index)
    }
  }

  def setField(row: MutableRow, ordinal: Int, index: Int)

}

class DoubleColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = DoubleType.JvmType
  val content = new Array[fieldType](rowNum)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setDouble(ordinal, get(index))
  }
}

class LongColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = LongType.JvmType
  val content = new Array[fieldType](rowNum)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setLong(ordinal, get(index))
  }
}

class IntColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = IntegerType.JvmType
  val content = new Array[fieldType](rowNum)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setInt(ordinal, get(index))
  }
}

class FloatColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = FloatType.JvmType
  val content = new Array[fieldType](rowNum)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setFloat(ordinal, get(index))
  }
}

class ShortColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = ShortType.JvmType
  val content = new Array[fieldType](rowNum)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setShort(ordinal, get(index))
  }
}

class ByteColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = ByteType.JvmType
  val content = new Array[fieldType](rowNum)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setByte(ordinal, get(index))
  }
}

class StringColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = String
  val content = new Array[fieldType](rowNum)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setString(ordinal, get(index))
  }
}

class BinaryColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = Array[Byte]
  val content = new Array[fieldType](rowNum)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.update(ordinal, get(index))
  }
}

class BooleanColumnVector(val rowNum: Int, val bs: BitSet) extends ColumnVector {
  type fieldType = Boolean
  def content = new Array[fieldType](0)
  override def get(i: Int) = bs.get(i)
  override def set(i: Int, v: fieldType) = bs.set(i, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setBoolean(ordinal, get(index))
  }
}

abstract class FakeColumnVector extends ColumnVector {
  def content = sys.error(s"Literal do not have content field")
  val rowNum = 0
}

case class DoubleLiteral(value: Double) extends FakeColumnVector {
  type fieldType = DoubleType.JvmType
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setDouble(ordinal, value)
  }
}

case class LongLiteral(val value: Long) extends FakeColumnVector {
  type fieldType = LongType.JvmType
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setLong(ordinal, value)
  }
}

case class IntLiteral(val value: Int) extends FakeColumnVector {
  type fieldType = IntegerType.JvmType
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setInt(ordinal, value)
  }
}

case class FloatLiteral(val value: Float) extends FakeColumnVector {
  type fieldType = FloatType.JvmType
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setFloat(ordinal, value)
  }
}

case class ShortLiteral(val value: Short) extends FakeColumnVector {
  type fieldType = ShortType.JvmType
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setShort(ordinal, value)
  }
}

case class ByteLiteral(val value: Byte) extends FakeColumnVector {
  type fieldType = ByteType.JvmType
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setByte(ordinal, value)
  }
}

case class StringLiteral(val value: String) extends FakeColumnVector {
  type fieldType = String
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setString(ordinal, value)
  }
}

case class BinaryLiteral(val value: Array[Byte]) extends FakeColumnVector {
  type fieldType = Array[Byte]
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.update(ordinal, value)
  }
}

case class BooleanLiteral(val value: Boolean) extends FakeColumnVector {
  type fieldType = Boolean
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setBoolean(ordinal, value)
  }
}

object ColumnVector {
  def apply(dt: DataType, rowNum: Int) = {
    dt match {
      case DoubleType => new DoubleColumnVector(rowNum)
      case LongType => new LongColumnVector(rowNum)
      case IntegerType => new IntColumnVector(rowNum)
      case FloatType => new FloatColumnVector(rowNum)
      case ShortType => new ShortColumnVector(rowNum)
      case ByteType => new ByteColumnVector(rowNum)
      case StringType => new StringColumnVector(rowNum)
      case BinaryType => new BinaryColumnVector(rowNum)
      case BooleanType => new BooleanColumnVector(rowNum, new BitSet(rowNum))
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