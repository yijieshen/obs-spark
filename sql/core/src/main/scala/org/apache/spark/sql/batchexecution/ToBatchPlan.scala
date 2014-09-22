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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.batchexecution.{SparkBatchPlan => BP}
import org.apache.spark.sql.catalyst.batchexpressions.{BatchExpression => BE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.columnar.InMemoryColumnarTableScan
import org.apache.spark.sql.execution._

private[sql] case class ToBatchPlan(sqlContext: SQLContext, rowNumInBatch: Int)
  extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case Filter(cond, child: BP) =>
      BatchFilter(cond, child)
    case Project(projectList, child: BP) =>
      BatchProject(projectList, child)
    case ExistingRdd(output, rdd) =>
      ExistingRowBatchRdd(output, rdd, rowNumInBatch)
    case InMemoryColumnarTableScan(attributes, relation) =>
      InMemoryColumnarBatchRdd(attributes, relation, rowNumInBatch)
  }
}


