package org.apache.spark.sql.catalyst.batchexpressions

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}

class RowBatch(val rowNum: Int, val ordinal: Int) {

  //TODO: substitute name to vector mapping into ordinal to column List?
  // the columns come from table or as result tuple
  val vectors = new Array[ColumnVector](ordinal)

  // selector for the current rowbatch
  var curSelector: BitSet = null

  var curRowNum: Int = _
}

object RowBatch {

  def buildFromAttributes(attrs: Seq[Attribute], rowNum: Int): RowBatch = {
    val rowBatch = new RowBatch(rowNum, attrs.length)
    attrs.zipWithIndex.foreach { case (attr, i) =>
      val ar = attr.asInstanceOf[AttributeReference]
      val cv = ColumnVector(ar.dataType, rowNum)
      rowBatch.vectors(i) = cv
    }
    rowBatch
  }
}
