package org.apache.spark.sql.batchexecution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.batchexpressions.codegen.{GenerateBatchPredicate, GenerateBatchMutableProjection}
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

      val nextRowBatch = RowBatch.buildFromAttributes(attributes, rowNum)
      val cvs = nextRowBatch.vectors

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
              cvs(i).setNullable(rc, nextRow(i))
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

  /**
   * A copy of execute method from InMemoryColumnarTableScan
   *
   * @return
   */
  override def execute() = {
    relation.cachedColumnBuffers.mapPartitions { iterator =>
      val columnBuffers = iterator.next()
      assert(!iterator.hasNext)

      new Iterator[Row] {
        // Find the ordinals of the requested columns.  If none are requested, use the first.
        val requestedColumns =
          if (attributes.isEmpty) Seq(0) else attributes.map(relation.output.indexOf(_))

        val columnAccessors = requestedColumns.map(columnBuffers(_)).map(ColumnAccessor(_))
        val nextRow = new GenericMutableRow(columnAccessors.length)

        override def next() = {
          var i = 0
          while (i < nextRow.length) {
            columnAccessors(i).extractTo(nextRow, i)
            i += 1
          }
          nextRow
        }

        override def hasNext = columnAccessors.head.hasNext
      }
    }
  }

}