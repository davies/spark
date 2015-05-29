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

package org.apache.spark.sql.catalyst.expressions.codegen

import scala.collection.mutable
import scala.language.existentials

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.codehaus.commons.compiler.CompilerFactoryFactory

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

// These classes are here to avoid issues with serialization and integration with quasiquotes.
class IntegerHashSet extends org.apache.spark.util.collection.OpenHashSet[Int]
class LongHashSet extends org.apache.spark.util.collection.OpenHashSet[Long]

/**
 * A base class for generators of byte code to perform expression evaluation.  Includes a set of
 * helpers for referring to Catalyst types and building trees that perform evaluation of individual
 * expressions.
 */
abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{universe => ru}
  import scala.tools.reflect.ToolBox

  protected val toolBox = runtimeMirror(getClass.getClassLoader).mkToolBox()
  protected val cbe = CompilerFactoryFactory.getDefaultCompilerFactory().newClassBodyEvaluator()

  protected val rowType = typeOf[Row]
  protected val exprType = typeOf[Expression]
  protected val mutableRowType = typeOf[MutableRow]
  protected val genericRowType = typeOf[GenericRow]
  protected val genericMutableRowType = typeOf[GenericMutableRow]

  protected val projectionType = typeOf[Projection]
  protected val mutableProjectionType = typeOf[MutableProjection]

  private val curId = new java.util.concurrent.atomic.AtomicInteger()
  private val javaSeparator = "$"

  /**
   * Can be flipped on manually in the console to add (expensive) expression evaluation trace code.
   */
  var debugLogging = false

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

  /** Binds an input expression to a given input schema */
  protected def bind(in: InType, inputSchema: Seq[Attribute]): InType

  def compile(code: String): Class[_] = {
    val startTime = System.currentTimeMillis()
    cbe.cook(code)
    val result = cbe.getClazz()
    val endTime = System.currentTimeMillis()
    def timeMs: Double = (endTime - startTime).toDouble
    logWarning(s"Code generated ${code.size} in $timeMs ms")
    result
  }

  /**
   * A cache of generated classes.
   *
   * From the Guava Docs: A Cache is similar to ConcurrentMap, but not quite the same. The most
   * fundamental difference is that a ConcurrentMap persists all elements that are added to it until
   * they are explicitly removed. A Cache on the other hand is generally configured to evict entries
   * automatically, in order to constrain its memory footprint.  Note that this cache does not use
   * weak keys/values and thus does not respond to memory pressure.
   */
  protected val cache = CacheBuilder.newBuilder()
    .maximumSize(1000)
    .build(
      new CacheLoader[InType, OutType]() {
        override def load(in: InType): OutType = globalLock.synchronized {
          val startTime = System.nanoTime()
          val result = create(in)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          logWarning(s"Code generated expression $in in $timeMs ms")
          result
        }
      })

  /** Generates the requested evaluator binding the given expression(s) to the inputSchema. */
  def generate(expressions: InType, inputSchema: Seq[Attribute]): OutType =
    generate(bind(expressions, inputSchema))

  /** Generates the requested evaluator given already bound expression(s). */
  def generate(expressions: InType): OutType = cache.get(canonicalize(expressions))

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
   * Scala ASTs for evaluating an [[Expression]] given a [[Row]] of input.
   *
   * @param code The sequence of statements required to evaluate the expression.
   * @param nullTerm A term that holds a boolean value representing whether the expression evaluated
   *                 to null.
   * @param primitiveTerm A term for a possible primitive value of the result of the evaluation. Not
   *                      valid if `nullTerm` is set to `true`.
   * @param objectTerm A possibly boxed version of the result of evaluating this expression.
   */
  protected case class EvaluatedExpression(
      code: String,
      nullTerm: TermName,
      primitiveTerm: TermName,
      objectTerm: TermName)

  /**
   * A context for codegen
   * @param borrowed the expressions that don't support codegen
   */
  case class CodeGenContext(borrowed: mutable.ArrayBuffer[Expression])

  /**
   * Create a new codegen context for expression evaluator, used to store those
   * expressions that don't support codegen
   */
  def newCodeGenContext(): CodeGenContext = {
    new CodeGenContext(new mutable.ArrayBuffer[Expression]())
  }

  /**
   * Given an expression tree returns an [[EvaluatedExpression]], which contains Scala trees that
   * can be used to determine the result of evaluating the expression on an input row.
   */
  def expressionEvaluator(e: Expression, ctx: CodeGenContext): EvaluatedExpression = {
    val primitiveTerm = freshName("primitiveTerm")
    val nullTerm = freshName("nullTerm")
    val objectTerm = freshName("objectTerm")

    implicit class Evaluate1(e: Expression) {
      def castOrNull(f: TermName => String, dataType: DataType): String = {
        val eval = expressionEvaluator(e, ctx)
        eval.code +
        s"""
          boolean $nullTerm = ${eval.nullTerm};
          ${primitiveForType(dataType)} $primitiveTerm = ${defaultPrimitive(dataType)};
          if (!$nullTerm) {
            $primitiveTerm = ${f(eval.primitiveTerm)};
          }
        """
      }
    }

    implicit class Evaluate2(expressions: (Expression, Expression)) {

      /**
       * Short hand for generating binary evaluation code, which depends on two sub-evaluations of
       * the same type.  If either of the sub-expressions is null, the result of this computation
       * is assumed to be null.
       *
       * @param f a function from two primitive term names to a tree that evaluates them.
       */
      def evaluate(f: (TermName, TermName) => String): String =
        evaluateAs(expressions._1.dataType)(f)

      def evaluateAs(resultType: DataType)(f: (TermName, TermName) => String): String = {
        // TODO: Right now some timestamp tests fail if we enforce this...
        if (expressions._1.dataType != expressions._2.dataType) {
          log.warn(s"${expressions._1.dataType} != ${expressions._2.dataType}")
        }

        val eval1 = expressionEvaluator(expressions._1, ctx)
        val eval2 = expressionEvaluator(expressions._2, ctx)
        val resultCode = f(eval1.primitiveTerm, eval2.primitiveTerm)

        eval1.code + eval2.code +
        s"""
          boolean $nullTerm = ${eval1.nullTerm} || ${eval2.nullTerm};
          ${primitiveForType(resultType)} $primitiveTerm = ${defaultPrimitive(resultType)};
          if(!$nullTerm) {
            $primitiveTerm = (${primitiveForType(resultType)})($resultCode);
          }
        """
      }
    }

    val inputTuple = newTermName(s"i")

    // TODO: Skip generation of null handling code when expression are not nullable.
    val primitiveEvaluation: PartialFunction[Expression, String] = {
      case b @ BoundReference(ordinal, dataType, nullable) =>
         s"""
          boolean $nullTerm = $inputTuple.isNullAt($ordinal);
          ${primitiveForType(dataType)} $primitiveTerm = $nullTerm ?
              ${defaultPrimitive(dataType)} : (${getColumn(inputTuple, dataType, ordinal)});
         """

      case expressions.Literal(null, dataType) =>
        s"""
          boolean $nullTerm = true;
          ${primitiveForType(dataType)} $primitiveTerm = ${defaultPrimitive(dataType)};
        """

      case expressions.Literal(value: UTF8String, dataType) =>
        val arr = s"new byte[]{${value.getBytes.map(_.toString).mkString(", ")}}"
        s"""
          boolean $nullTerm = ${value == null};
          org.apache.spark.sql.types.UTF8String $primitiveTerm =
            new org.apache.spark.sql.types.UTF8String().set(${arr});
         """
      case expressions.Literal(value, FloatType) =>
        s"""
          boolean $nullTerm = ${value == null};
          float $primitiveTerm = ${value}f;
         """
      // TODO(davies): other literal types

      case expressions.Literal(value, dataType) =>
        s"""
          boolean $nullTerm = ${value == null};
          ${primitiveForType(dataType)} $primitiveTerm = $value;
         """

      case Cast(child @ BinaryType(), StringType) =>
        child.castOrNull(c =>
          s"new org.apache.spark.sql.types.UTF8String().set($c)",
          StringType)

      case Cast(child @ DateType(), StringType) =>
        child.castOrNull(c =>
          s"""new org.apache.spark.sql.types.UTF8String().set(
                org.apache.spark.sql.catalyst.util.DateUtils.toString($c))""",
          StringType)

      case Cast(child @ BooleanType(), dt: NumericType) =>
        child.castOrNull(c => s"(${primitiveForType(dt)})($c?1:0)", dt)

      case Cast(child @ NumericType(), ByteType) =>
        child.castOrNull(c => s"(byte)($c)", ByteType)

      case Cast(child @ NumericType(), ShortType) =>
        child.castOrNull(c => s"(short)($c)", ShortType)

      case Cast(child @ NumericType(), IntegerType) =>
        child.castOrNull(c => s"(int)($c)", IntegerType)

      case Cast(child @ NumericType(), LongType) =>
        child.castOrNull(c => s"(long)($c)", LongType)

      case Cast(child @ NumericType(), DoubleType) =>
        child.castOrNull(c => s"(double)($c)", DoubleType)

      case Cast(child @ NumericType(), FloatType) =>
        child.castOrNull(c => s"(float)($c)", FloatType)

      // Special handling required for timestamps in hive test cases since the toString function
      // does not match the expected output.
      case Cast(e, StringType) if e.dataType != TimestampType =>
        e.castOrNull(c =>
          s"new org.apache.spark.sql.types.UTF8String().set($c.toString())",
          StringType)

      case EqualTo(e1 @ BinaryType(), e2 @ BinaryType()) =>
        (e1, e2).evaluateAs (BooleanType) {
          case (eval1, eval2) =>
            s"java.util.Arrays.equals((byte[])$eval1, (byte[])$eval2)"
        }

      case EqualTo(e1, e2) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => s"$eval1 == $eval2" }

      /* TODO: Fix null semantics.
      case In(e1, list) if !list.exists(!_.isInstanceOf[expressions.Literal]) =>
        val eval = expressionEvaluator(e1)

        val checks = list.map {
          case expressions.Literal(v: String, dataType) =>
            q"if(${eval.primitiveTerm} == $v) return true"
          case expressions.Literal(v: Int, dataType) =>
            q"if(${eval.primitiveTerm} == $v) return true"
        }

        val funcName = newTermName(s"isIn${curId.getAndIncrement()}")

        """
            def $funcName: Boolean = {
              ..${eval.code}
              if(${eval.nullTerm}) return false
              ..$checks
              return false
            }
            val $nullTerm = false
            val $primitiveTerm = $funcName
        """
      */

      case GreaterThan(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => s"$eval1 > $eval2" }
      case GreaterThanOrEqual(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => s"$eval1 >= $eval2" }
      case LessThan(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => s"$eval1 < $eval2" }
      case LessThanOrEqual(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => s"$eval1 <= $eval2" }

      case And(e1, e2) =>
        val eval1 = expressionEvaluator(e1, ctx)
        val eval2 = expressionEvaluator(e2, ctx)
        // TODO(davies): This is different than And.eval()
        s"""
          ${eval1.code}
          boolean $nullTerm = false;
          boolean $primitiveTerm  = false;

          if (!${eval1.nullTerm} && !${eval1.primitiveTerm}) {
          } else {
            ${eval2.code}
            if (!${eval2.nullTerm} && !${eval2.primitiveTerm}) {
            } else if (!${eval1.nullTerm} && !${eval2.nullTerm}) {
              $primitiveTerm = true;
            } else {
              $nullTerm = true;
            }
          }
         """

      case Or(e1, e2) =>
        val eval1 = expressionEvaluator(e1, ctx)
        val eval2 = expressionEvaluator(e2, ctx)

        s"""
          ${eval1.code}
          boolean $nullTerm = false;
          boolean $primitiveTerm = false;

          if (!${eval1.nullTerm} && ${eval1.primitiveTerm}) {
            $primitiveTerm = true;
          } else {
            ${eval2.code}
            if (!${eval2.nullTerm} && ${eval2.primitiveTerm}) {
              $primitiveTerm = true;
            } else if (!${eval1.nullTerm} && !${eval2.nullTerm}) {
              $primitiveTerm = false;
            } else {
              $nullTerm = true;
            }
          }
         """

      case Not(child) =>
        // Uh, bad function name...
        child.castOrNull(c => s"!$c", BooleanType)

      case Add(e1, e2) if e1.dataType != DecimalType() =>
        (e1, e2) evaluate { case (eval1, eval2) => s"$eval1 + $eval2" }
      case Subtract(e1, e2) if e1.dataType != DecimalType() =>
        (e1, e2) evaluate { case (eval1, eval2) => s"$eval1 - $eval2" }
      case Multiply(e1, e2) if e1.dataType != DecimalType() =>
        (e1, e2) evaluate { case (eval1, eval2) => s"$eval1 * $eval2" }
      case Divide(e1, e2) if e1.dataType != DecimalType() =>
        val eval1 = expressionEvaluator(e1, ctx)
        val eval2 = expressionEvaluator(e2, ctx)

        eval1.code + eval2.code +
        s"""
          boolean $nullTerm = false;
          ${primitiveForType(e1.dataType)} $primitiveTerm = 0;

          if (${eval1.nullTerm} || ${eval2.nullTerm} || ${eval2.primitiveTerm} == 0) {
            $nullTerm = true;
          } else {
            $primitiveTerm = ${eval1.primitiveTerm} / ${eval2.primitiveTerm};
          }
         """

      case Remainder(e1, e2) if e1.dataType != DecimalType() =>
        val eval1 = expressionEvaluator(e1, ctx)
        val eval2 = expressionEvaluator(e2, ctx)

        eval1.code + eval2.code +
        s"""
          boolean $nullTerm = false;
          ${primitiveForType(e1.dataType)} $primitiveTerm = 0;

          if (${eval1.nullTerm} || ${eval2.nullTerm} || ${eval2.primitiveTerm} == 0) {
            $nullTerm = true;
          } else {
            $primitiveTerm = ${eval1.primitiveTerm} % ${eval2.primitiveTerm};
          }
         """

      case IsNotNull(e) =>
        val eval = expressionEvaluator(e, ctx)
        s"""
          ${eval.code}
          boolean $nullTerm = false;
          boolean $primitiveTerm = !${eval.nullTerm};
        """

      case IsNull(e) =>
        val eval = expressionEvaluator(e, ctx)
        s"""
          ${eval.code}
          boolean $nullTerm = false;
          boolean $primitiveTerm = ${eval.nullTerm};
        """

      case e @ Coalesce(children) =>
        s"""
          boolean $nullTerm = true;
          ${primitiveForType(e.dataType)} $primitiveTerm = ${defaultPrimitive(e.dataType)};
        """ +
        children.map { c =>
          val eval = expressionEvaluator(c, ctx)
          s"""
            if($nullTerm) {
              ${eval.code}
              if(!${eval.nullTerm}) {
                $nullTerm = false;
                $primitiveTerm = ${eval.primitiveTerm};
              }
            }
          """
        }.mkString("\n")

      case e @ expressions.If(condition, trueValue, falseValue) =>
        val condEval = expressionEvaluator(condition, ctx)
        val trueEval = expressionEvaluator(trueValue, ctx)
        val falseEval = expressionEvaluator(falseValue, ctx)

        s"""
          boolean $nullTerm = false;
          ${primitiveForType(e.dataType)} $primitiveTerm = ${defaultPrimitive(e.dataType)};
          ${condEval.code}
          if(!${condEval.nullTerm} && ${condEval.primitiveTerm}) {
            ${trueEval.code}
            $nullTerm = ${trueEval.nullTerm};
            $primitiveTerm = ${trueEval.primitiveTerm};
          } else {
            ${falseEval.code}
            $nullTerm = ${falseEval.nullTerm};
            $primitiveTerm = ${falseEval.primitiveTerm};
          }
        """

      case NewSet(elementType) =>
        s"""
          boolean $nullTerm = false;
          ${hashSetForType(elementType)} $primitiveTerm = new ${hashSetForType(elementType)}();
        """

      case AddItemToSet(item, set) =>
        val itemEval = expressionEvaluator(item, ctx)
        val setEval = expressionEvaluator(set, ctx)

        val elementType = set.dataType.asInstanceOf[OpenHashSetUDT].elementType
        val htype = hashSetForType(elementType)

        itemEval.code + setEval.code +
        s"""
           if (!${itemEval.nullTerm} && !${setEval.nullTerm}) {
             (($htype)${setEval.primitiveTerm}).add(${itemEval.primitiveTerm});
           }
           boolean $nullTerm = false;
           ${htype} $primitiveTerm = ($htype)${setEval.primitiveTerm};
         """

      case CombineSets(left, right) =>
        val leftEval = expressionEvaluator(left, ctx)
        val rightEval = expressionEvaluator(right, ctx)

        val elementType = left.dataType.asInstanceOf[OpenHashSetUDT].elementType
        val htype = hashSetForType(elementType)

        leftEval.code + rightEval.code +
        s"""
          boolean $nullTerm = false;
          ${htype} $primitiveTerm =
            (${htype})${leftEval.primitiveTerm};
          $primitiveTerm.union((${htype})${rightEval.primitiveTerm});
        """

      case MaxOf(e1, e2) if e1.dataType != DecimalType() =>
        val eval1 = expressionEvaluator(e1, ctx)
        val eval2 = expressionEvaluator(e2, ctx)

        eval1.code + eval2.code +
        s"""
          boolean $nullTerm = false;
          ${primitiveForType(e1.dataType)} $primitiveTerm = ${defaultPrimitive(e1.dataType)};

          if (${eval1.nullTerm}) {
            $nullTerm = ${eval2.nullTerm};
            $primitiveTerm = ${eval2.primitiveTerm};
          } else if (${eval2.nullTerm}) {
            $nullTerm = ${eval1.nullTerm};
            $primitiveTerm = ${eval1.primitiveTerm};
          } else {
            if (${eval1.primitiveTerm} > ${eval2.primitiveTerm}) {
              $primitiveTerm = ${eval1.primitiveTerm};
            } else {
              $primitiveTerm = ${eval2.primitiveTerm};
            }
          }
        """

      case MinOf(e1, e2) if e1.dataType != DecimalType() =>
        val eval1 = expressionEvaluator(e1, ctx)
        val eval2 = expressionEvaluator(e2, ctx)

        eval1.code + eval2.code +
        s"""
          boolean $nullTerm = false;
          ${primitiveForType(e1.dataType)} $primitiveTerm = ${defaultPrimitive(e1.dataType)};

          if (${eval1.nullTerm}) {
            $nullTerm = ${eval2.nullTerm};
            $primitiveTerm = ${eval2.primitiveTerm};
          } else if (${eval2.nullTerm}) {
            $nullTerm = ${eval1.nullTerm};
            $primitiveTerm = ${eval1.primitiveTerm};
          } else {
            if (${eval1.primitiveTerm} < ${eval2.primitiveTerm}) {
              $primitiveTerm = ${eval1.primitiveTerm};
            } else {
              $primitiveTerm = ${eval2.primitiveTerm};
            }
          }
        """

      case UnscaledValue(child) =>
        val childEval = expressionEvaluator(child, ctx)

        childEval.code +
        s"""
         boolean $nullTerm = ${childEval.nullTerm};
         long $primitiveTerm = $nullTerm ? -1 : ${childEval.primitiveTerm}.toUnscaledLong();
         """

      case MakeDecimal(child, precision, scale) =>
        val eval = expressionEvaluator(child, ctx)

        eval.code +
        s"""
         boolean $nullTerm = ${eval.nullTerm};
         org.apache.spark.sql.types.Decimal $primitiveTerm = ${defaultPrimitive(DecimalType())};

         if (!$nullTerm) {
           $primitiveTerm = new org.apache.spark.sql.types.Decimal();
           $primitiveTerm = $primitiveTerm.setOrNull(${eval.primitiveTerm}, $precision, $scale);
           $nullTerm = $primitiveTerm == null;
         }
         """
    }

    // If there was no match in the partial function above, we fall back on calling the interpreted
    // expression evaluator.
    val code: String =
      primitiveEvaluation.lift.apply(e).getOrElse {
        logError(s"No rules to generate $e")
        ctx.borrowed += e
        s"""
          // expression: ${e}
          Object $objectTerm = expressions[${ctx.borrowed.size - 1}].eval(i);
          boolean $nullTerm = $objectTerm == null;
          ${primitiveForType(e.dataType)} $primitiveTerm = ${defaultPrimitive(e.dataType)};
          if (!$nullTerm) $primitiveTerm = (${termForType(e.dataType)})$objectTerm;
         """
      }

    // Only inject debugging code if debugging is turned on.
    val debugCode =
      if (debugLogging) {
        val localLogger = log
        val localLoggerTree = reify { localLogger }
        s"""
          $localLoggerTree.debug(
            ${e.toString} + ": " + (if ($nullTerm) "null" else $primitiveTerm.toString))
        """
      } else {
        ""
      }

    EvaluatedExpression(code + debugCode, nullTerm, primitiveTerm, objectTerm)
  }

  protected def getColumn(inputRow: TermName, dataType: DataType, ordinal: Int) = {
    dataType match {
      case StringType => s"(org.apache.spark.sql.types.UTF8String)$inputRow.apply($ordinal)"
      case dt: DataType if isNativeType(dt) => s"$inputRow.${accessorForType(dt)}($ordinal)"
      case _ => s"(${termForType(dataType)})$inputRow.apply($ordinal)"
    }
  }

  protected def setColumn(
      destinationRow: TermName,
      dataType: DataType,
      ordinal: Int,
      value: TermName) = {
    dataType match {
      case StringType => s"$destinationRow.update($ordinal, $value)"
      case dt: DataType if isNativeType(dt) =>
        s"$destinationRow.${mutatorForType(dt)}($ordinal, $value)"
      case _ => s"$destinationRow.update($ordinal, $value)"
    }
  }

  protected def accessorForType(dt: DataType) = dt match {
    case IntegerType => "getInt"
    case other => s"get${termForType(dt)}"
  }

  protected def mutatorForType(dt: DataType) = dt match {
    case IntegerType => "setInt"
    case other => s"set${termForType(dt)}"
  }

  protected def hashSetForType(dt: DataType) = dt match {
    case IntegerType => typeOf[IntegerHashSet]
    case LongType => typeOf[LongHashSet]
    case unsupportedType =>
      sys.error(s"Code generation not support for hashset of type $unsupportedType")
  }

  protected def primitiveForType(dt: DataType): String = dt match {
    case IntegerType => "int"
    case LongType => "long"
    case ShortType => "short"
    case ByteType => "byte"
    case DoubleType => "double"
    case FloatType => "float"
    case BooleanType => "boolean"
    case DecimalType() => "org.apache.spark.sql.types.Decimal"
    case BinaryType => "byte[]"
    case StringType => "org.apache.spark.sql.types.UTF8String"
    case DateType => "int"
    case TimestampType => "java.sql.Timestamp"
    case _ => "Object"
  }

  protected def defaultPrimitive(dt: DataType): String = dt match {
    case BooleanType => "false"
    case FloatType => "-1.0f"
    case ShortType => "-1"
    case LongType => "-1"
    case ByteType => "-1"
    case DoubleType => "-1.0"
    case IntegerType => "-1"
    case DateType => "-1"
    case DecimalType() => "null"
    case StringType =>  "null"
    case _ => "null"
  }

  protected def termForType(dt: DataType): String = dt match {
    case IntegerType => "Integer"
    case LongType => "Long"
    case ShortType => "Short"
    case ByteType => "Byte"
    case DoubleType => "Double"
    case FloatType => "Float"
    case BooleanType => "Boolean"
    case DecimalType() => "org.apache.spark.sql.types.Decimal"
    case BinaryType => "byte[]"
    case StringType => "org.apache.spark.sql.types.UTF8String"
    case DateType => "Integer"
    case TimestampType => "java.sql.Timestamp"
//    case udt: UserDefinedType[_] =>
//      udt.userClass.getCanonicalName
    case _ =>
      "Object"
  }

  /**
   * List of data types that have special accessors and setters in [[Row]].
   */
  protected val nativeTypes =
    Seq(IntegerType, BooleanType, LongType, DoubleType, FloatType, ShortType, ByteType)

  /**
   * Returns true if the data type has a special accessor and setter in [[Row]].
   */
  protected def isNativeType(dt: DataType) = nativeTypes.contains(dt)
}
