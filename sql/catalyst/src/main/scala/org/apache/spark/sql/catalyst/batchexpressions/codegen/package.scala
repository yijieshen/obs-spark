package org.apache.spark.sql.catalyst.batchexpressions

/**
 * A collection of generators that build custom bytecode at runtime for performing the batch evaluation
 * of catalyst expression.
 */
package object codegen {

  /**
   * A lock to protect invoking the scala compiler at runtime, since it is not thread safe in Scala
   * 2.10.
   */
  protected[codegen] val globalLock = org.apache.spark.sql.catalyst.ScalaReflectionLock

}
