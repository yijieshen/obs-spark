package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}

import scala.collection.mutable.Map

class RowBatch(val rowNum: Int) {

  // the columns come from table or as result tuple
  val name2Vector = Map.empty[String, ColumnVector]

  // selector for the current rowbatch
  var curSelector: BitSet = null

  var curRowNum: Int = _
}

object RowBatch {

  def buildFromAttributes(attrs: Seq[Attribute], rowNum: Int): RowBatch = {
    val rowBatch = new RowBatch(rowNum)
    attrs.foreach { attr =>
      val ar = attr.asInstanceOf[AttributeReference]
      val cv = ColumnVector(ar.dataType, rowNum)
      rowBatch.name2Vector(ar.name) = cv
    }
    rowBatch
  }

  def getColumnVectors(attrs: Seq[Attribute], rowBatch: RowBatch): Array[ColumnVector] = {
    val rst = new Array[ColumnVector](attrs.length)
    attrs.zipWithIndex.foreach {
      case (attr: AttributeReference, i) =>
        rst(i) = rowBatch.name2Vector(attr.name)
      case (bbr: BatchBoundReference, i) =>
        rst(i) = rowBatch.name2Vector(bbr.name)
    }
    rst
  }

}
