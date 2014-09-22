/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.catalyst.types._

abstract class ColumnVector {

  def rowNum: Int
  type fieldType <: Any

  def getInt(i: Int): Int = ???
  def setInt(i: Int, v: Int): Unit = ???

  def getLong(i: Int): Long = ???
  def setLong(i: Int, v: Long): Unit = ???

  def getDouble(i: Int): Double = ???
  def setDouble(i: Int, v: Double): Unit = ???

  def getFloat(i: Int): Float = ???
  def setFloat(i: Int, v: Float): Unit = ???

  def getBoolean(i: Int): Boolean = ???
  def setBoolean(i: Int, v: Boolean): Unit = ???

  def getShort(i: Int): Short = ???
  def setShort(i: Int, v: Short): Unit = ???

  def getByte(i: Int): Byte = ???
  def setByte(i: Int, v: Byte): Unit = ???

  def getString(i: Int): String = ???
  def setString(i: Int, v: String): Unit = ???

  def get(i: Int): fieldType = ???
  def set(i: Int, v: fieldType): Unit = ???

  //(null -> 0) (notNull -> 1)
  var notNullArray: BitSet = _

  //TODO: better way to do this, how to avoid boxing & downcasting?
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
  type fieldType = Double
  val content = new Array[Double](rowNum)

  override def getDouble(i: Int) = content(i)
  override def setDouble(i: Int, v: Double) = content(i) = v
  override def get(i: Int) = content(i)
  override def set(i: Int, v: Double) = content(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setDouble(ordinal, getDouble(index))
  }
}

class LongColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = Long
  val content = new Array[Long](rowNum)

  override def getLong(i: Int): Long = content(i)
  override def setLong(i: Int, v: Long): Unit = content(i) = v
  override def get(i: Int) = content(i)
  override def set(i: Int, v: Long) = content(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setLong(ordinal, getLong(index))
  }
}

class IntColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = Int
  val content = new Array[Int](rowNum)

  override def getInt(i: Int): Int = content(i)
  override def setInt(i: Int, v: Int): Unit = content(i) = v
  override def get(i: Int) = content(i)
  override def set(i: Int, v: Int) = content(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setInt(ordinal, getInt(index))
  }
}

class FloatColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = Float
  val content = new Array[Float](rowNum)

  override def getFloat(i: Int): Float = content(i)
  override def setFloat(i: Int, v: Float): Unit = content(i) = v
  override def get(i: Int) = content(i)
  override def set(i: Int, v: Float) = content(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setFloat(ordinal, getFloat(index))
  }
}

class ShortColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = Short
  val content = new Array[Short](rowNum)

  override def getShort(i: Int): Short = content(i)
  override def setShort(i: Int, v: Short): Unit = content(i) = v
  override def get(i: Int) = content(i)
  override def set(i: Int, v: Short) = content(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setShort(ordinal, getShort(index))
  }
}

class ByteColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = Byte
  val content = new Array[Byte](rowNum)

  override def getByte(i: Int): Byte = content(i)
  override def setByte(i: Int, v: Byte): Unit = content(i) = v
  override def get(i: Int) = content(i)
  override def set(i: Int, v: Byte) = content(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setByte(ordinal, getByte(index))
  }
}

class StringColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = String
  val content = new Array[String](rowNum)

  override def getString(i: Int): String = content(i)
  override def setString(i: Int, v: String): Unit = content(i) = v
  override def get(i: Int) = content(i)
  override def set(i: Int, v: String) = content(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setString(ordinal, getString(index))
  }
}

class BinaryColumnVector(val rowNum: Int) extends ColumnVector {
  type fieldType = Array[Byte]
  val content = new Array[fieldType](rowNum)

  override def get(i: Int) = content(i)
  override def set(i: Int, v: fieldType) = content(i) = v

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.update(ordinal, get(index))
  }
}

class BooleanColumnVector(val rowNum: Int, val bs: BitSet) extends ColumnVector {
  type fieldType = Boolean
  override def getBoolean(i: Int) = bs.get(i)
  override def setBoolean(i: Int, v: Boolean) = bs.set(i, v)
  override def get(i: Int) = bs.get(i)
  override def set(i: Int, v: Boolean) = bs.set(i, v)

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setBoolean(ordinal, getBoolean(index))
  }
}

abstract class FakeColumnVector extends ColumnVector {
  def content = sys.error(s"Literal do not have content field")
  val rowNum = 0
}

case class DoubleLiteral(value: Double) extends FakeColumnVector {
  type fieldType = Double
  override def getDouble(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setDouble(ordinal, value)
  }
}

case class LongLiteral(value: Long) extends FakeColumnVector {
  type fieldType = Long
  override def getLong(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setLong(ordinal, value)
  }
}

case class IntLiteral(value: Int) extends FakeColumnVector {
  type fieldType = Int
  override def getInt(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setInt(ordinal, value)
  }
}

case class FloatLiteral(value: Float) extends FakeColumnVector {
  type fieldType = Float
  override def getFloat(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setFloat(ordinal, value)
  }
}

case class ShortLiteral(value: Short) extends FakeColumnVector {
  type fieldType = Short
  override def getShort(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setShort(ordinal, value)
  }
}

case class ByteLiteral(value: Byte) extends FakeColumnVector {
  type fieldType = Byte
  override def getByte(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setByte(ordinal, value)
  }
}

case class StringLiteral(value: String) extends FakeColumnVector {
  type fieldType = String
  override def getString(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.setString(ordinal, value)
  }
}

case class BinaryLiteral(value: Array[Byte]) extends FakeColumnVector {
  type fieldType = Array[Byte]
  override def get(i: Int) = value

  override def setField(row: MutableRow, ordinal: Int, index: Int): Unit = {
    row.update(ordinal, value)
  }
}

case class BooleanLiteral(value: Boolean) extends FakeColumnVector {
  type fieldType = Boolean
  override def getBoolean(i: Int) = value

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
