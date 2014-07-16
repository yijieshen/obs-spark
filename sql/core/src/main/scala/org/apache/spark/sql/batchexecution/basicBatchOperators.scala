package org.apache.spark.sql.batchexecution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Attribute, NamedExpression, Row}
import org.apache.spark.sql.columnar.{ColumnAccessor, InMemoryRelation}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class BatchProject(projectList: Seq[NamedExpression], child: SparkBatchPlan)
  extends UnaryBatchNode {

  override def output = projectList.map(_.toAttribute)

  override def batchExecute() = child.batchExecute().mapPartitions { iter =>
    @transient val batchProj = new BatchProjection(projectList)
    iter.map(batchProj)
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class BatchFilter(condition: BatchExpression, child: SparkBatchPlan) extends UnaryBatchNode {
  override def output = child.output

  override def batchExecute() = child.batchExecute().mapPartitions { iter =>
    iter.map{ rowBatch =>
      val selector = condition.eval(rowBatch).asInstanceOf[BooleanColumnVector].bitset
      rowBatch.curSelector = selector
      rowBatch
    }
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
      val cvs = RowBatch.getColumnVectors(attributes, nextRowBatch)
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
              cvs(i).set(rc, row(i))
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

      val nextRowBatch = RowBatch.buildFromAttributes(attributes, rowNum)
      val cvs = RowBatch.getColumnVectors(attributes, nextRowBatch)

      val columnBuffers = iterator.next()
      assert(!iterator.hasNext)

      val requestedColumns =
        if (attributes.isEmpty) Seq(0) else attributes.map(relation.output.indexOf(_))

      val columnAccessors = requestedColumns.map(columnBuffers(_)).map(ColumnAccessor(_))
      val nextRow = new GenericMutableRow(columnAccessors.length)

      new Iterator[RowBatch] {
        override def hasNext: Boolean = columnAccessors.head.hasNext

        override def next(): RowBatch = {
          cvs.foreach(_.reinit)
          var rc = 0
          val rowNum = nextRowBatch.rowNum
          while (columnAccessors.head.hasNext && rc < rowNum) {
            var i = 0
            while (i < nextRow.length) {
              columnAccessors(i).extractTo(nextRow, i)
              cvs(i).set(rc, nextRow(i))
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
}