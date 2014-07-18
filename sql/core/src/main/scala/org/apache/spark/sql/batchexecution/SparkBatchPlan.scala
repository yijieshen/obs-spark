package org.apache.spark.sql.batchexecution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.batchexpressions.{ColumnVector, RowBatch}
import org.apache.spark.sql.catalyst.expressions.{MutableRow, GenericMutableRow}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, LeafNode, SparkPlan, UnaryNode}

@DeveloperApi
trait SparkBatchPlan extends SparkPlan {
  self: Product =>

  def execute(): RDD[Row] = batchExecute().mapPartitions { batchIter =>

    new Iterator[Row] {
      var nextRowBatch: RowBatch = null
      var nextCVs: Array[ColumnVector] = null
      var rowCountInRB = 0
      var curRowNumInRB = 0

      def getNextRowBatch(): Boolean = {
        curRowNumInRB = 0
        if(batchIter.hasNext) {
          nextRowBatch = batchIter.next()
          rowCountInRB = nextRowBatch.curRowNum
          nextCVs = RowBatch.getColumnVectors(output, nextRowBatch)
          true
        } else {
          nextRowBatch = null
          rowCountInRB = 0
          nextCVs = null
          false
        }
      }

      def extractRow(nextRow: MutableRow, curRowNum: Int): Unit ={
        var i = 0
        while (i < outSize) {
          nextCVs(i).extractTo(nextRow, i, curRowNum)
          i += 1
        }
      }

      override def hasNext: Boolean = {
        //initialize
        if(nextRowBatch == null) {
          val hasNextRB = getNextRowBatch
          if(!hasNextRB) return false
        }

        while(nextRowBatch != null) {
          val curSelector = nextRowBatch.curSelector
          if(curSelector != null) 
        }
        false
      }



      val outSize = output.size
      val nextRow = new GenericMutableRow(outSize)

      override def next(): Row = nextRow
      //      override def next(): Row = {
      //        var i = 0
      //        while (i < outSize) {
      //          nextCVs(i).extractTo(nextRow, i, curRowNumInRB)
      //          i += 1
      //        }
      //        curRowNumInRB += 1
      //        nextRow
      //      }
      //      override def hasNext: Boolean = {
      //        if(nextRowBatch == null) {
      //          getNextRowBatch
      //        } else if(curRowNumInRB < rowCountInRB) {
      //          true
      //        } else if(curRowNumInRB == rowCountInRB) {
      //          getNextRowBatch
      //        } else {
      //          false
      //        }
      //      }
    }
  }

  def batchExecute(): RDD[RowBatch]

  override def executeCollect(): Array[Row] = execute().map(_.copy).collect()
}

private[sql] trait LeafBatchNode extends SparkBatchPlan with LeafNode {
  self: Product =>
}

private[sql] trait UnaryBatchNode extends SparkBatchPlan with UnaryNode {
  self: Product =>
  override def outputPartitioning: Partitioning = child.outputPartitioning
}

private[sql] trait BinaryBatchNode extends SparkBatchPlan with BinaryNode {
  self: Product =>
}