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

package org.apache.spark.sql.batchexecution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.batchexpressions.{IntIterator, BitSet, ColumnVector, RowBatch}
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, MutableRow, Row}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, LeafNode, SparkPlan, UnaryNode}

@DeveloperApi
trait SparkBatchPlan extends SparkPlan {
  self: Product =>

  def execute(): RDD[Row] = batchExecute().mapPartitions { batchIter =>

    new Iterator[Row] {
      var nextRowBatch: RowBatch = null
      var nextCVs: Array[ColumnVector] = null
      var nextSelectors: BitSet = null
      var rowCountInRB = 0
      var curRowNumInRB = 0
      var curIterator: IntIterator = null

      def getNextRowBatch(): Boolean = {
        curRowNumInRB = 0
        if(batchIter.hasNext) {
          nextRowBatch = batchIter.next()
          rowCountInRB = nextRowBatch.curRowNum
          nextCVs = nextRowBatch.vectors
          nextSelectors = nextRowBatch.curSelector
          if(nextSelectors != null) curIterator = nextSelectors.iterator
          true
        } else {
          nextRowBatch = null
          rowCountInRB = 0
          nextCVs = null
          nextSelectors = null
          curIterator = null
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

      val outSize = output.size
      val nextRow = new GenericMutableRow(outSize)

      override def next(): Row = nextRow

      override def hasNext: Boolean = {
        //initialize
        if(nextRowBatch == null) {
          getNextRowBatch
        }

        while(nextRowBatch != null) {
          if(nextSelectors != null) {
            if(curIterator.hasNext) {
              curRowNumInRB = curIterator.next()
              extractRow(nextRow, curRowNumInRB)
              return true
            } else {
              getNextRowBatch
            }
          } else {
            if(curRowNumInRB < rowCountInRB) {
              extractRow(nextRow, curRowNumInRB)
              curRowNumInRB += 1
              return true
            } else {
              getNextRowBatch
            }
          }
        }
        return false
      }
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
