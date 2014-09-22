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

import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.expressions._

object GenerateBatchPredicate extends BatchCodeGenerator[Expression, (RowBatch) => RowBatch]{

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  override protected def canonicalize(in: Expression): Expression =
    ExpressionCanonicalizer(in)

  override protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  override protected def create(predicate: Expression): ((RowBatch) => RowBatch) = {
    val selector = freshName("selector")
    val eval = expressionEvaluator(predicate)

    val code =
      q"""
        (input: $rowBatchType) => {
          ..${eval.code}
          val $selector = ${eval.cvTerm}.asInstanceOf[${typeOf[BooleanColumnVector]}].bs
          input.curSelector = $selector
          input
        }
      """
//    println(show(code))
    toolBox.eval(code).asInstanceOf[RowBatch => RowBatch]
  }
}
