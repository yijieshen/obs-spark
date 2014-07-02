package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.types._

import scala.collection.mutable.Map

class RowBatch(val rowNum: Int) {

  val name2Vector = Map.empty[String, ColumnVector[DataType]]

  val memPool = new MemoryPool(rowNum)

//  def getTmpVector(dt: DataType) = {
//    val tmpV = ColumnVector.getNewCV(dt, rowNum, true)
//    dt match {
//      case DoubleType | LongType | IntegerType | FloatType | ShortType | ByteType =>
//        tmpV.setContent(memPool)
//      case BooleanType | BinaryType | StringType =>
//        throw new UnsupportedOperationException(s"How to handle $dt ?")
//    }
//    tmpV
//  }

  def getTmpMemory(width: Int) = memPool.borrowMemory(width)

  def getTmpVector(dt: DataType) = {
    ColumnVector.getNewCV(dt, rowNum, true)
  }

  def free() = {
    name2Vector.values.foreach(_.content.free())
    memPool.free()
  }

}

object RowBatch {
}
