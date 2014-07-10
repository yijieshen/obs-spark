package org.apache.spark.sql.batchexecution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Row, Attribute, NamedExpression}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class BatchProject(projectList: Seq[NamedExpression], child: SparkBatchPlan)
  extends UnaryBatchNode {

  override def output = projectList.map(_.toAttribute)

  override def execute() = child.execute().mapPartitions { iter =>
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

  override def execute() = child.execute().mapPartitions { iter =>
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
case class ExistingRowBatchRdd(output: Seq[Attribute], rdd: RDD[Row], rowNum: Int)
  extends LeafBatchNode {
  override def execute(): RDD[RowBatch] = {
    rdd.mapPartitions { iterator =>
      val nextRowBatch = RowBatch.buildFromAttributes(output, rowNum)

      new Iterator[RowBatch] {
        override def next() = {

        }
        override def hasNext =
      }
    }
  }
}