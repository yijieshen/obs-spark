package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.{Literal, Row}
import org.apache.spark.sql.catalyst.types._

import scala.collection.mutable.Map

class RowBatch(val rowNum: Int) {

  // the columns come from table or as result tuple
  val name2Vector = Map.empty[String, ColumnVector]

  val name2Literal = Map.empty[String, Literal]

  // selector for the current rowbatch
  var curSelector: BitSet = null

  val memPool = new MemoryPool(rowNum)

  def getTmpMemory(width: Int) = memPool.borrowMemory(width)

  def getVector(dt: DataType, isTmp: Boolean = false) = {
    ColumnVector.getNewCV(dt, rowNum, isTmp)
  }

  def free() = {
    name2Vector.values.foreach(_.content.free())
    memPool.free()
  }

  def expand : Array[Row] = ???

}

object RowBatch {
}
