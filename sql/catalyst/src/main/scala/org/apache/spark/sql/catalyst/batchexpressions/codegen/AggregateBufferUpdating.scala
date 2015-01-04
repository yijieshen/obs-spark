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

package org.apache.spark.sql.catalyst.batchexpressions.codegen

import org.apache.spark.sql.catalyst.batchexpressions.AggregateBufferUpdate
import org.apache.spark.sql.catalyst.batchexpressions.codegen.AGG
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

import scala.collection.mutable.MutableList

object AggregateBufferUpdating
  extends NewBatchCodeGenerator[Seq[AGG], ()=> AggregateBufferUpdate] {

  def updateCode(aggs: Seq[AGG]): Seq[Tree] = {

    val codes: Seq[(Seq[Tree], Seq[Tree])] = aggs.map(_.code)
    val prepares: Seq[Tree] = codes.flatMap(_._1)
    val cals: Seq[Tree] = codes.flatMap(_._2)

    prepares ++
    q"""

     """

    null
  }



  def transformAggs(exprs: Seq[Expression]): (Seq[AGG], Seq[Expression], Int) = {
    var i = 0
    var currentLength = 0
    var currentExpr = exprs(0)
    val length = exprs.length
    val resultAGGs = new MutableList[AGG]
    val resultProjs = new MutableList[Expression]
    while (i < length) {

      currentExpr match {
        case Sum(expr) =>
          resultProjs += expr
          resultAGGs +=
            SUM(expr.dataType, currentLength, i + 1/* the first cv is the combined key*/)
          currentLength = currentLength + dtLength(expr.dataType)
        case Count(expr) =>
          resultProjs += expr
          resultAGGs +=
            COUNT(expr.dataType, currentLength, i + 1)
          currentLength = currentLength + 8 + dtLength(expr.dataType)
        case Max(expr) =>
          resultProjs += expr
          resultAGGs +=
            MAX(expr.dataType, currentLength, i + 1)
          currentLength = currentLength + dtLength(expr.dataType)
        case Min(expr) =>
          resultProjs += expr
          resultAGGs +=
            MIN(expr.dataType, currentLength, i + 1)
          currentLength = currentLength + dtLength(expr.dataType)
      }

      i += 1
      currentExpr = exprs(i)
    }
    (resultAGGs.toSeq, resultProjs.toSeq, currentLength)
  }

  def dtLength(dataType: DataType): Int = {
    dataType match {
      case LongType => 8
      case IntegerType => 4
      case ShortType => 2
      case FloatType => 4
      case DoubleType => 8
    }
  }

}
