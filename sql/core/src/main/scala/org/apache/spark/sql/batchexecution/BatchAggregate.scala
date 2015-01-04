package org.apache.spark.sql.batchexecution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.batchexpressions.codegen.AggregateBufferUpdating.AGG
import org.apache.spark.sql.catalyst.batchexpressions.codegen._
import org.apache.spark.sql.catalyst.expressions._

case class BatchAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkBatchPlan) extends UnaryBatchNode {

  val grouping: Expression = Concat(groupingExpressions)
  val aggregates = aggregateExpressions diff groupingExpressions
  val aggregatesToCompute = aggregates.flatMap { a =>
    a.collect { case agg: AggregateExpression => agg}
  }

  val aggInfo = AggregateBufferUpdating.transformAggs(aggregatesToCompute)
  val aggs: Seq[AGG] = aggInfo._1
  val aggProjs: Seq[Expression] = aggInfo._2
  val aggBufferLength: Int = aggInfo._3


  val projs: Seq[Expression] = aggProjs.+:(grouping)

  @transient lazy val projFunc = NewGenerateBatchMutableProjection(projs, child.output)
  @transient lazy val bufferPrepare = AggregateBufferPreparation(aggBufferLength)

  def batchExecute() = ???

  override def output = ???

  override def execute(): RDD[Row] = {

    if(partial) {
      if (groupingExpressions.isEmpty) {
        null
      } else {
        child.batchExecute().mapPartitions { iter =>
          val hashTable = new ByteArrayMap
          var currentRowBatch: RowBatch = null
          while (iter.hasNext) {
            currentRowBatch = iter.next()
            val singleKeyRowBatch = projFunc()(currentRowBatch)
            val keyCV = singleKeyRowBatch.vectors(0)
            val aggregateSlots: BinaryColumnVector =
              bufferPrepare(currentRowBatch)()(currentRowBatch, hashTable)
          }





          null
        }
      }
    } else { //not partial
      null
    }
  }

}
