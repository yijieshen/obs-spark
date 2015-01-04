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

package org.apache.spark.sql

import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.test._

case class SYJT(key: String, a: Int, b: Double, c: Int)

object SYJT {

  val t1: SchemaRDD = TestSQLContext.sparkContext.parallelize(
    (1 to 100).map(i => SYJT(s"val_$i", i + 100, i + 200, i + 300)))
  t1.registerTempTable("t1")
  //TestSQLContext.cacheTable("t1")

  def main(args: Array[String]) {
    val srdd = sql("SELECT a, b, avg(a+c*b*a) from t1 group by a, b")
    srdd.collect().foreach(println)
    println(
      s"""
         |== Logical Plan ==
         |${srdd.logicalPlan}
         |== Analyzed Plan ==
         |${srdd.queryExecution.analyzed}
         |== Physical Plan ==
         |${srdd.queryExecution.nonBatchPlan}
         |== Batch Plan ==
         |${srdd.queryExecution.executedPlan}
       """.stripMargin)
  }

}
