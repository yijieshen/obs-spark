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

import java.nio.ByteBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.batchexpressions.codegen.{GenerateBatchPredicate, GenerateBatchMutableProjection}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.columnar.{ColumnAccessor, InMemoryRelation}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class BatchProject(projectList: Seq[NamedExpression], child: SparkBatchPlan)
  extends UnaryBatchNode {
  override def output = projectList.map(_.toAttribute)

  @transient lazy val buildProjection = GenerateBatchMutableProjection(projectList, child.output)

  override def batchExecute() = child.batchExecute().mapPartitions { iter =>
    val projection = buildProjection()
    iter.map(projection)
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class BatchFilter(condition: Expression, child: SparkBatchPlan) extends UnaryBatchNode {
  override def output = child.output

  @transient lazy val conditionEvaluator = GenerateBatchPredicate(condition, child.output)

  override def batchExecute() = child.batchExecute().mapPartitions { iter =>
    iter.map(conditionEvaluator)
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class ExistingRowBatchRdd(attributes: Seq[Attribute], rdd: RDD[Row], rowNum: Int)
  extends LeafBatchNode {

  override def output: Seq[Attribute] = attributes

  override def batchExecute(): RDD[RowBatch] = {
    rdd.mapPartitions { iterator =>
      val nextRowBatch = RowBatch.buildFromAttributes(attributes, rowNum)
      val cvs = nextRowBatch.vectors
      var row: Row = null
      new Iterator[RowBatch] {

        override def hasNext = iterator.hasNext

        override def next() = {
          cvs.foreach(_.reinit)
          var rc = 0
          val rowNum = nextRowBatch.rowNum
          while (iterator.hasNext && rc < rowNum) {
            row = iterator.next()
            var i = 0
            while (i < row.length) {
              cvs(i).setNullable(rc, row(i))
              i += 1
            }
            rc += 1
          }
          nextRowBatch.curRowNum = rc
          nextRowBatch
        }
      }
    }
  }

  override def execute() = rdd

}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class InMemoryColumnarBatchRdd(
    attributes: Seq[Attribute], relation: InMemoryRelation, rowNum: Int)
  extends LeafBatchNode {

  override def output: Seq[Attribute] = attributes

  override def batchExecute(): RDD[RowBatch] = {
    relation.cachedColumnBuffers.mapPartitions { iterator =>

      val requestedColumns =
        if (attributes.isEmpty) {
          Seq(0)
        } else {
          attributes.map(a => relation.output.indexWhere(_.exprId == a.exprId))
        }

      val nextRowBatch = RowBatch.buildFromAttributes(attributes, rowNum)
      val cvs = nextRowBatch.vectors

      new Iterator[RowBatch] {
        var columnBuffers: Array[ByteBuffer] = null
        var columnAccessors: Seq[ColumnAccessor] = null

        def nextBatch() = {
          columnBuffers = iterator.next()
          columnAccessors = requestedColumns.map(columnBuffers(_)).map(ColumnAccessor(_))
        }

        override def hasNext = iterator.hasNext

        override def next(): RowBatch = {
          nextBatch()
          cvs.foreach(_.reinit)
          val rowCount = columnAccessors.zip(cvs).map {
            case (ca, cv) => ca.fill(cv)
          }.head
          nextRowBatch.curRowNum = rowCount
          nextRowBatch
        }
      }
    }
  }

  /**
   * A copy of execute method from InMemoryColumnarTableScan
   *
   * @return
   */
  override def execute() = {
    relation.cachedColumnBuffers.mapPartitions { iterator =>
      // Find the ordinals of the requested columns.  If none are requested, use the first.
      val requestedColumns =
        if (attributes.isEmpty) {
          Seq(0)
        } else {
          attributes.map(a => relation.output.indexWhere(_.exprId == a.exprId))
        }

      new Iterator[Row] {
        private[this] var columnBuffers: Array[ByteBuffer] = null
        private[this] var columnAccessors: Seq[ColumnAccessor] = null
        nextBatch()

        private[this] val nextRow = new GenericMutableRow(columnAccessors.length)

        def nextBatch() = {
          columnBuffers = iterator.next()
          columnAccessors = requestedColumns.map(columnBuffers(_)).map(ColumnAccessor(_))
        }

        override def next() = {
          if (!columnAccessors.head.hasNext) {
            nextBatch()
          }

          var i = 0
          while (i < nextRow.length) {
            columnAccessors(i).extractTo(nextRow, i)
            i += 1
          }
          nextRow
        }

        override def hasNext = columnAccessors.head.hasNext || iterator.hasNext
      }
    }
  }

}
