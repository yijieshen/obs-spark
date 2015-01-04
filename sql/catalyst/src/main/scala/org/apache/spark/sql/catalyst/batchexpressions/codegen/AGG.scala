package org.apache.spark.sql.catalyst.batchexpressions.codegen

import org.apache.spark.sql.catalyst.types.DataType

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

abstract class AGG extends NewBatchCodeGenerator {
  def code: (Seq[Tree], Seq[Tree])

  def posInBuffer: Int

  def indexInRB: Int

  def childDT: DataType

  val getter = accessorForType(childDT)
  val setter = pMutatorForType(childDT)
  val inputCV = freshName("inputCV")
  val inputRB = newTermName("inputRB")
  val notNullArrayTerm = freshName("notNullArrayTerm")
  val prepareCode: Seq[Tree] =
    q"""
        val $inputCV = ${getCV(inputRB, indexInRB)}.asInstanceOf[${getCVType(childDT)}]
        //val $notNullArrayTerm = $inputCV.notNullArray
      """ :: Nil

}

case class SUM(childDT: DataType, posInBuffer: Int, indexInRB: Int) extends AGG {
  def code = {
    val calculationCode: Seq[Tree] =
      q"""
          buffer.$setter($posInBuffer, buffer.$getter($posInBuffer) + $inputCV.$getter(i))
        """ :: Nil
    (prepareCode, calculationCode)
  }
}

case class COUNT(childDT: DataType, posInBuffer: Int, indexInRB: Int) extends AGG {
  def code = {
    val calculationCode: Seq[Tree] =
      q"""
          buffer.putLong($posInBuffer, buffer.getLong($posInBuffer) + 1)
        """ :: Nil
    (prepareCode, calculationCode)
  }
}

case class MAX(childDT: DataType, posInBuffer: Int, indexInRB: Int) extends AGG {
  def code = {
    val calculationCode: Seq[Tree] =
      q"""
          val state = buffer.$getter($posInBuffer)
          val current = $inputCV.$getter(i)
          if (state < current)
            buffer.$setter($posInBuffer, current)
        """.children
    (prepareCode, calculationCode)
  }
}

case class MIN(childDT: DataType, posInBuffer: Int, indexInRB: Int) extends AGG {
  def code = {
    val calculationCode: Seq[Tree] =
      q"""
          val state = buffer.$getter($posInBuffer)
          val current = $inputCV.$getter(i)
          if (state > current)
            buffer.$setter($posInBuffer, current)
        """.children
    (prepareCode, calculationCode)
  }
}
