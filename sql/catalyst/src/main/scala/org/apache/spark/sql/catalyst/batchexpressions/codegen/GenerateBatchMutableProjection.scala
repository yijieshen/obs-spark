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

import org.apache.spark.sql.catalyst.batchexpressions.RBProjection
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._

object GenerateBatchMutableProjection
  extends BatchCodeGenerator[Seq[Expression], () => RBProjection] {

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  val outputRowBatch = newTermName(s"output")

  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer(_))

  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  override protected def create(expressions: Seq[Expression]): (() => RBProjection) = {
    val projectionCode = expressions.zipWithIndex.flatMap { case(e, i) =>
      val evaluationCode = expressionEvaluator(e)
      evaluationCode.code :+
      q"""
        ${setCV(outputRowBatch, i, evaluationCode.cvTerm)}
      """
    }
    val code =
      q"""
        () => { new $rbProjectionType {
            private[this] var $outputRowBatch: $rowBatchType = null

            def apply(input: $rowBatchType): $rowBatchType = {
              if($outputRowBatch == null) {
                $outputRowBatch = new $rowBatchType(input.rowNum, ${expressions.size})
              }
              ..$projectionCode
              output.curRowNum = input.curRowNum
              output.curSelector = input.curSelector
              output
            }
          }
        }
       """
//    println(show(code))
    toolBox.eval(code).asInstanceOf[() => RBProjection]
  }

}
