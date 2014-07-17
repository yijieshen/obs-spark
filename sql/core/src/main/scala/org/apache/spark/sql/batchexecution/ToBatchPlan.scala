package org.apache.spark.sql.batchexecution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.batchexecution.{SparkBatchPlan => BP}
import org.apache.spark.sql.catalyst.batchexpressions.{BatchExpression => BE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.columnar.InMemoryColumnarTableScan
import org.apache.spark.sql.execution._

private[sql] case class ToBatchPlan(sqlContext: SQLContext, rowNumInBatch: Int)
  extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case Filter(cond: BE, child: BP) =>
      BatchFilter(cond, child)
    case Project(projectList, child: BP) =>
      BatchProject(projectList, child)
    case ExistingRdd(output, rdd) =>
      ExistingRowBatchRdd(output, rdd, rowNumInBatch)
    case InMemoryColumnarTableScan(attributes, relation) =>
      InMemoryColumnarBatchRdd(attributes, relation, rowNumInBatch)
  }
}


