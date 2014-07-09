package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.Row

import scala.collection.mutable.Map

class RowBatch(val rowNum: Int) {

  // the columns come from table or as result tuple
  val name2Vector = Map.empty[String, ColumnVector]

  // selector for the current rowbatch
  var curSelector: BitSet = null

  val memPool = new OffHeapMemoryPool(rowNum)

  def getTmpMemory(width: Int): OffHeapMemory =
    memPool.borrowMemory(width)

  def returnMemory(width: Int, mem: OffHeapMemory) =
    memPool.returnMemory(width, mem)

  def free() = {
    name2Vector.values.foreach(_.content.free())
    memPool.free()
  }

  def expand: Array[Row] = ???

}

object RowBatch {
}
