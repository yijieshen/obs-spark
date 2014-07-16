package org.apache.spark.sql.batchexecution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.batchexpressions.RowBatch
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, LeafNode, SparkPlan, UnaryNode}

@DeveloperApi
trait SparkBatchPlan extends SparkPlan{
  self: Product =>

  override def execute(): RDD[Row] =
    sys.error(s"SparkPlan: $this should use batchExecute() instead")

  def batchExecute(): RDD[RowBatch]

  /**
   * Runs this query returning the result as an array.
   */
  override def executeCollect(): Array[Row] = batchExecute().map(_.expand).collect().flatten
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