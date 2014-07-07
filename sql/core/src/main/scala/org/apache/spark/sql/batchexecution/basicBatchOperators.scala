package org.apache.spark.sql.batchexecution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.expressions.NamedExpression

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
      val selector = condition.eval(rowBatch).asInstanceOf[BooleanColumnVector].getSelector
      rowBatch.curSelector = selector
      rowBatch
    }
  }
}