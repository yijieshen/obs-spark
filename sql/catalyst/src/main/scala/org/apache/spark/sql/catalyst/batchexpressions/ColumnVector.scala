package org.apache.spark.sql.catalyst.batchexpressions

import com.google.common.base.{Objects => GObjs}
import org.apache.spark.sql.catalyst.types._

abstract class ColumnVector (val typeWidth: Int, val rowNum: Int, val isTemp: Boolean = false) {

  def dt: DataType

  //TODO: do we need nullable?
  var nullable: Boolean = false
  //(null -> 0) (notNull -> 1)
  var notNullArray: BitSet = _

  def setNullable(notNullbm: BitSet) = {
    nullable = true
    notNullArray = notNullbm
  }

  //TODO: Currently, we don't use these two fields for per columnVector selector,
  //maybe an optimization later, use private[this] to hide
  private[this] var withSelector: Boolean = false
  private[this] var selector: BitSet = _

  private[this] def setSelector(sbm: BitSet) = {
    withSelector = true
    selector = sbm
  }

  //content
  var content: Memory = _

  def setContent(pool: MemoryPool) = {
    content = pool.borrowMemory(typeWidth)
  }

  def setContent(mem: Memory) = {
    content = mem
  }

  override def toString =
    GObjs.toStringHelper(this)
      .add("nullable", nullable)
      .add("withSelector", withSelector)
      .toString
}


class DoubleColumnVector(rowNum: Int, isTemp: Boolean) extends ColumnVector(8, rowNum, isTemp) {
  val dt = DoubleType
}

class LongColumnVector(rowNum: Int, isTemp: Boolean) extends ColumnVector(8, rowNum, isTemp) {
  val dt = LongType
}

class IntColumnVector(rowNum: Int, isTemp: Boolean) extends ColumnVector(4, rowNum, isTemp) {
  val dt = IntegerType
}

class FloatColumnVector(rowNum: Int, isTemp: Boolean) extends ColumnVector(4, rowNum, isTemp) {
  val dt = FloatType
}

class ShortColumnVector(rowNum: Int, isTemp: Boolean) extends ColumnVector(2, rowNum, isTemp) {
  val dt = ShortType
}

class ByteColumnVector(rowNum: Int, isTemp: Boolean) extends ColumnVector(1, rowNum, isTemp) {
  val dt = ByteType
}

class BooleanColumnVector(rowNum: Int) extends ColumnVector(0, rowNum) {
  val dt = BooleanType
  override def setContent(pool: MemoryPool) =
    throw new IllegalArgumentException(
      "Setting String Column Through MemoryPool are not supported currently")

  def getSelector = content.asInstanceOf[BooleanMemory].bs
}

class BinaryColumnVector(rowNum: Int) extends ColumnVector(0, rowNum) {
  val dt = BinaryType
  override def setContent(pool: MemoryPool) =
    throw new IllegalArgumentException(
      "Setting String Column Through MemoryPool are not supported currently")
}

class StringColumnVector(rowNum: Int) extends ColumnVector(0, rowNum) {
  val dt = StringType
  override def setContent(pool: MemoryPool) =
    throw new IllegalArgumentException(
      "Setting String Column Through MemoryPool are not supported currently")
}


object ColumnVector {
  def getNewCV(dt: DataType, rowNum: Int, isTmp: Boolean) = {
    dt match {
      case DoubleType => new DoubleColumnVector(rowNum, isTmp)
      case LongType => new LongColumnVector(rowNum, isTmp)
      case IntegerType => new IntColumnVector(rowNum, isTmp)
      case FloatType => new FloatColumnVector(rowNum, isTmp)
      case ShortType => new ShortColumnVector(rowNum, isTmp)
      case ByteType => new ByteColumnVector(rowNum, isTmp)
      case BooleanType => new BooleanColumnVector(rowNum)
      case BinaryType => new BinaryColumnVector(rowNum)
      case StringType => new StringColumnVector(rowNum)
    }
  }
}