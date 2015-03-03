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

import java.nio.ByteBuffer

import com.google.common.cache.{CacheLoader, CacheBuilder}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.batchexpressions._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._

abstract class NewBatchCodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{universe => ru}
  import scala.tools.reflect.ToolBox

  protected val toolBox = runtimeMirror(getClass.getClassLoader)
      .mkToolBox(/*options = "-Ydump-classes /Users/yijie/abc/"*/)

  protected val rowBatchType = typeOf[RowBatch]
  protected val rbProjectionType = typeOf[RBProjection]
  protected val abPrepareType = typeOf[AggregateBufferPrepare]

  protected val doubleLiteralType = typeOf[DoubleLiteral]
  protected val longLiteralType = typeOf[LongLiteral]
  protected val intLiteralType = typeOf[IntLiteral]
  protected val stringLiteralType = typeOf[StringLiteral]
  protected val booleanLiteralType = typeOf[BooleanLiteral]
  protected val binaryColumnVectorType = typeOf[BinaryColumnVector]

  protected val aggregateBufferRefType = typeOf[AggregateBufferRef]

  protected val byteBufferType = typeOf[ByteBuffer]
  protected val byteArrayMapType = typeOf[ByteArrayMap]
  protected val hashMapType = typeOf[java.util.HashMap[Row, MutableRow]]

  protected val outputRowBatch = newTermName(s"output")
  protected val outputBinaryCV = newTermName(s"outputBCV")

  protected val columnVectorObj = reify(ColumnVector)

  private val curId = new java.util.concurrent.atomic.AtomicInteger()
  private val javaSeparator = "$"

  /**
   * Generates a class for a given input expression.  Called when there is not cached code
   * already available.
   */
  protected def create(in: InType): OutType

  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  protected def canonicalize(in: InType): InType

  /** Binds an input expression to a given input schema,
    *  i.e. transform expressions with AttributeReference into exprs with BoundReference*/
  protected def bind(in: InType, inputSchema: Seq[Attribute]): InType

  /**
   * A cache of generated classes.
   *
   * From the Guava Docs: A Cache is similar to ConcurrentMap, but not quite the same. The most
   * fundamental difference is that a ConcurrentMap persists all elements that are added to it until
   * they are explicitly removed. A Cache on the other hand is generally configured to evict entries
   * automatically, in order to constrain its memory footprint
   */
  protected val cache = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
      new CacheLoader[InType, OutType]() {
        override def load(in: InType): OutType = globalLock.synchronized {
          val startTime = System.nanoTime()
          val result = create(in)
          val endTime = System.nanoTime()
          def timeMs = (endTime - startTime).toDouble / 1000000
          logInfo(s"Code generated expression $in in $timeMs ms")
          result
        }
      })

  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def apply(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    apply(bind(expressions, inputSchema))

  /** Generates the requested evaluator given already bound expression(s). */
  def apply(expressions: InType): OutType = cache.get(canonicalize(expressions))

  /**
   * Returns a term name that is unique within this instance of a `CodeGenerator`.
   *
   * (Since we aren't in a macro context we do not seem to have access to the built in `freshName`
   * function.)
   */
  protected def freshName(prefix: String): TermName = {
    newTermName(s"$prefix$javaSeparator${curId.getAndIncrement}")
  }

  /**
   * Scala ASTs for evaluating an
   */
  protected case class EvaluatedExpression(
      prepareCode: Seq[Tree],
      calculationCode: Tree,
      notNullArrayTerm: TermName)

  def expressionEvaluator(e: Expression): EvaluatedExpression = {
    val notNullArrayTerm = freshName("notNullArrayTerm") //result cv's notnullarry
    val inputCV = freshName("inputCV")
    val inputRowBatch = newTermName(s"input")

    implicit class Evaluate1(e: Expression) {
      def cast(f: Tree => Tree, resultType: DataType): (Seq[Tree], Tree) = {
        val eval = expressionEvaluator(e)

        val nna = eval.notNullArrayTerm
        val calculationCode = f(eval.calculationCode)

        val prepareCode = eval.prepareCode :+
          q"""
          val $notNullArrayTerm = if($nna != null) $nna.copy else null
          """
        (prepareCode, calculationCode)
      }
    }

    implicit class Evaluate2(expressions: (Expression, Expression)) {
      def evaluate(f: (Tree, Tree) => Tree): (Seq[Tree], Tree) =
        evaluateAs(expressions._1.dataType)(f)

      def evaluateAs(resultType: DataType)(f: (Tree, Tree) => Tree): (Seq[Tree], Tree) = {
        val eval1 = expressionEvaluator(expressions._1)
        val eval2 = expressionEvaluator(expressions._2)

        val lnna = eval1.notNullArrayTerm
        val rnna = eval2.notNullArrayTerm

        val calculationCode = f(eval1.calculationCode, eval2.calculationCode)

        val prepareCode = eval1.prepareCode ++ eval2.prepareCode :+
        q"""
          val $notNullArrayTerm = ${andWithNull(lnna, rnna, true)}
        """
        (prepareCode, calculationCode)
      }
    }

    implicit class EvaluateN(expressions: Seq[Expression]) {
      def cancat(): (Seq[Tree], Tree) = {
        //TODO: Handle null case in cancatenation
        val evals = expressions.map(expressionEvaluator)

        val nnas = evals.map(_.notNullArrayTerm)
        val nna = q"val $notNullArrayTerm = null"

        val prepareCode = evals.flatMap(_.prepareCode) :+ nna

        val exprWithCalCode = expressions.zip(evals.map(_.calculationCode))
        val calculationSeq = exprWithCalCode.map(a=> append(a._1, a._2))
        val calculationCode = q"..$calculationSeq"
        (prepareCode, calculationCode)
      }

      def append(expr: Expression, calCode: Tree) = {
        expr.dataType match {
          case StringType =>
            val bytes = freshName("stringBytes")
            q"""
              val $bytes = $calCode.getBytes
              buffer.putInt($bytes.length).put($bytes, 0, $bytes.length)
            """
          case IntegerType =>
            q"buffer.putInt($calCode)"
          case ShortType =>
            q"buffer.putShort($calCode)"
          case ByteType =>
            q"buffer.put($calCode)"
          case LongType =>
            q"buffer.put($calCode)"
          case FloatType =>
            q"buffer.putFloat($calCode)"
          case DoubleType =>
            q"buffer.putDouble($calCode)"
        }
      }

    }

    val primitiveEvaluation: PartialFunction[Expression, (Seq[Tree], Tree)] = {

      case b@BoundReference(ordinal, dataType, nullable) =>
        val getter = accessorForType(dataType)
        val prepareCode =
          q"""
          val $inputCV = ${getCV(inputRowBatch, ordinal)}.asInstanceOf[${getCVType(dataType)}]
          val $notNullArrayTerm = $inputCV.notNullArray
        """.children
        val calculationCode = q"""$inputCV.$getter(i)"""
        (prepareCode, calculationCode)

      //TODO: Literal handling
      //TODO: Cast handling(Binary2String, timestamp etc)
      //TODO: remainder(%)

      case expressions.Literal(value: Boolean, dataType) =>
        val getter = accessorForType(dataType)
        val prepareCode = q"""
          val $inputCV = new $booleanLiteralType($value)
          val $notNullArrayTerm = $inputCV.notNullArray
         """.children
        val calculationCode = q"""$inputCV.$getter(i)"""
        (prepareCode, calculationCode)

      case expressions.Literal(value: String, dataType) =>
        val getter = accessorForType(dataType)
        val prepareCode = q"""
          val $inputCV = new $stringLiteralType($value)
          val $notNullArrayTerm = $inputCV.notNullArray
         """.children
        val calculationCode = q"""$inputCV.$getter(i)"""
        (prepareCode, calculationCode)

      case expressions.Literal(value: Int, dataType) =>
        val getter = accessorForType(dataType)
        val prepareCode = q"""
          val $inputCV = new $intLiteralType($value)
          val $notNullArrayTerm = $inputCV.notNullArray
         """.children
        val calculationCode = q"""$inputCV.$getter(i)"""
        (prepareCode, calculationCode)

      case expressions.Literal(value: Long, dataType) =>
        val getter = accessorForType(dataType)
        val prepareCode = q"""
          val $inputCV = new $longLiteralType($value)
          val $notNullArrayTerm = $inputCV.notNullArray
         """.children
        val calculationCode = q"""$inputCV.$getter(i)"""
        (prepareCode, calculationCode)

      case expressions.Literal(value: Double, dataType) =>
        val getter = accessorForType(dataType)
        val prepareCode = q"""
          val $inputCV = new $doubleLiteralType($value)
          val $notNullArrayTerm = $inputCV.notNullArray
         """.children
        val calculationCode = q"""$inputCV.$getter(i)"""
        (prepareCode, calculationCode)

      case Cast(child@NumericType(), IntegerType) =>
        child.cast(c => q"$c.toInt", IntegerType)

      case Cast(child@NumericType(), LongType) =>
        child.cast(c => q"$c.toLong", LongType)

      case Cast(child@NumericType(), DoubleType) =>
        child.cast(c => q"$c.toDouble", DoubleType)

      case Cast(child@NumericType(), FloatType) =>
        child.cast(c => q"$c.toFloat", IntegerType)

      case Add(e1, e2) => (e1, e2) evaluate { (v1, v2) => q"$v1 + $v2"}
      case Subtract(e1, e2) => (e1, e2) evaluate { (v1, v2) => q"$v1 - $v2"}
      case Multiply(e1, e2) => (e1, e2) evaluate { (v1, v2) => q"$v1 * $v2"}
      case Divide(e1, e2) => (e1, e2) evaluate { (v1, v2) => q"$v1 / $v2"}
      case Remainder(e1@IntegerType(), e2@IntegerType()) =>
        (e1, e2) evaluate { (v1, v2) => q"$v1 % $v2"}

      case EqualTo(e1, e2) =>
        (e1, e2).evaluateAs(BooleanType) { (v1, v2) => q"$v1 == $v2"}
      case GreaterThan(e1@NumericType(), e2@NumericType()) =>
        (e1, e2).evaluateAs(BooleanType) { (v1, v2) => q"$v1 > $v2"}
      case GreaterThanOrEqual(e1@NumericType(), e2@NumericType()) =>
        (e1, e2).evaluateAs(BooleanType) { (v1, v2) => q"$v1 >= $v2"}
      case LessThan(e1@NumericType(), e2@NumericType()) =>
        (e1, e2).evaluateAs(BooleanType) { (v1, v2) => q"$v1 < $v2"}
      case LessThanOrEqual(e1@NumericType(), e2@NumericType()) =>
        (e1, e2).evaluateAs(BooleanType) { (v1, v2) => q"$v1 <= $v2"}

      case Concat(exprs) =>
        EvaluateN(exprs).cancat
    }

    val (prepareCode, calculationCode) =
      primitiveEvaluation.apply(e)

    EvaluatedExpression(prepareCode, calculationCode, notNullArrayTerm)
  }

  def projectionCode(expressions: Seq[Expression]) = {
    expressions.zipWithIndex.flatMap { case (e, index) =>
      e match {
        //BoundReference as a single Expression, just copy reference
        case BoundReference(ordinal, dataType, nullable) =>
          val inputCV = freshName("inputCV")
          val inputRowBatch = newTermName(s"input")
          q"""
              val $inputCV = ${getCV(inputRowBatch, ordinal)}.asInstanceOf[${getCVType(dataType)}]
              ${setCV(outputRowBatch, index, inputCV)}
            """.children

          /*
        case c @ Concat(children) =>
          val eval = expressionEvaluator(c)

          val cvResult = freshName("cvresult")
          val notNullArrayTerm = freshName("notNullArrayTerm")
          val selector = freshName("selector")
          val bitmap = freshName("bitmap")
          val bmIter = freshName("bmIter")
          val rowNum = freshName("curRowNum")

          eval.prepareCode ++
            q"""
              val $cvResult = new $binaryColumnVectorType(input.curRowNum)
              val $notNullArrayTerm = ${eval.notNullArrayTerm}
              val $selector = input.curSelector
              val $bitmap = ${andWithNull(notNullArrayTerm, selector, false)}
              val buffer = $byteBufferType.allocate(10)

               if ($bitmap != null) {
                $bitmap.availableBits = input.curRowNum
                val $bmIter = $bitmap.iterator
                var i = 0
                while ($bmIter.hasNext) {
                  i = $bmIter.next()
                  ${eval.calculationCode}
                  $cvResult.set(i, buffer.array().clone())
                  buffer.clear()
                }
              } else {
                val $rowNum = input.curRowNum
                var i = 0
                while (i < $rowNum) {
                  ${eval.calculationCode}
                  $cvResult.set(i, buffer.array().clone())
                  buffer.clear()
                  i += 1
                }
              }
              $cvResult.notNullArray = $notNullArrayTerm
              ${setCV(outputRowBatch, index, cvResult)}
            """.children
            */

        case _ =>
          val eval = expressionEvaluator(e)
          val setter = mutatorForType(e.dataType)

          val cvResult = freshName("cvresult")
          val notNullArrayTerm = freshName("notNullArrayTerm")
          val selector = freshName("selector")
          val bitmap = freshName("bitmap")
          val bmIter = freshName("bmIter")
          val rowNum = freshName("curRowNum")
          val resultDt = reify(e.dataType)

          eval.prepareCode ++
            q"""
              val $cvResult = $columnVectorObj.apply($resultDt, input.curRowNum)
              val $notNullArrayTerm = ${eval.notNullArrayTerm}
              val $selector = input.curSelector
              val $bitmap = ${andWithNull(notNullArrayTerm, selector, false)}

              if ($bitmap != null) {
                $bitmap.availableBits = input.curRowNum
                val $bmIter = $bitmap.iterator
                var i = 0
                while ($bmIter.hasNext) {
                  i = $bmIter.next()
                  $cvResult.$setter(i, ${eval.calculationCode})
                }
              } else {
                val $rowNum = input.curRowNum
                var i = 0
                while (i < $rowNum) {
                  $cvResult.$setter(i, ${eval.calculationCode})
                  i += 1
                }
              }
              $cvResult.notNullArray = $notNullArrayTerm
              ${setCV(outputRowBatch, index, cvResult)}
            """.children
      }
    }
  }


  protected def getCV(inputRB: TermName, ordinal: Int) = {
    q"$inputRB.vectors($ordinal)"
  }

  protected def setCV(outputRB: TermName, ordinal: Int, value: TermName) = {
    q"$outputRB.vectors($ordinal) = $value"
  }

  protected def accessorForType(dt: DataType) = newTermName(s"get${primitiveForType(dt)}")
  protected def mutatorForType(dt: DataType) = newTermName(s"set${primitiveForType(dt)}")
  protected def pMutatorForType(dt: DataType) = newTermName(s"put${primitiveForType(dt)}")

  protected def primitiveForType(dt: DataType) = dt match {
    case IntegerType => "Int"
    case LongType => "Long"
    case ShortType => "Short"
    case ByteType => "Byte"
    case DoubleType => "Double"
    case FloatType => "Float"
    case BooleanType => "Boolean"
    case StringType => "String"
  }

  protected def getCVType(dt: DataType) = dt match {
    case LongType => typeOf[LongColumnVector]
    case IntegerType => typeOf[IntColumnVector]
    case ShortType => typeOf[ShortColumnVector]
    case ByteType => typeOf[ByteColumnVector]
    case DoubleType => typeOf[DoubleColumnVector]
    case FloatType => typeOf[FloatColumnVector]
    case BooleanType => typeOf[BooleanColumnVector]
    case StringType => typeOf[StringColumnVector]
  }

  protected def andWithNull(bs1: TermName, bs2: TermName, cp: Boolean): Tree = {
    q"""
      if ($bs1 != null && $bs2 != null) {
      $bs1 & $bs2
    } else if ($bs1 != null && $bs2 == null) {
      if($cp) $bs1.copy else $bs1
    } else if ($bs2 != null && $bs1 == null) {
      if($cp) $bs2.copy else $bs2
    } else {
      null
    }
     """
  }
}
