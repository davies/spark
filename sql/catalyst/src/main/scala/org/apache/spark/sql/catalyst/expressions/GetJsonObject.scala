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

package org.apache.spark.sql.catalyst.expressions

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.core._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{StringType, DataType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.parsing.combinator.RegexParsers

private[this] sealed trait PathInstruction
private[this] object PathInstruction {
  private[expressions] case object Subscript extends PathInstruction
  private[expressions] case object Wildcard extends PathInstruction
  private[expressions] case object Key extends PathInstruction
  private[expressions] case class Index(index: Long) extends PathInstruction
  private[expressions] case class Named(name: String) extends PathInstruction
}

private[this] sealed trait WriteStyle
private[this] object WriteStyle {
  private[expressions] case object RawStyle extends WriteStyle
  private[expressions] case object QuotedStyle extends WriteStyle
  private[expressions] case object FlattenStyle extends WriteStyle
}

private[this] object JsonPathParser extends RegexParsers {
  import PathInstruction._

  def root: Parser[Char] = '$'

  def long: Parser[Long] = "\\d+".r ^? {
    case x => x.toLong
  }

  // parse `[*]` and `[123]` subscripts
  def subscript: Parser[List[PathInstruction]] =
    for {
      operand <- '[' ~> ('*' ^^^ Wildcard | long ^^ Index) <~ ']'
    } yield {
      Subscript :: operand :: Nil
    }

  // parse `.name` or `['name']` child expressions
  def named: Parser[List[PathInstruction]] =
    for {
      name <- '.' ~> "[^\\.\\[]+".r | "[\\'" ~> "[^\\'\\?]+" <~ "\\']"
    } yield {
      Key :: Named(name) :: Nil
    }

  // child wildcards: `..`, `.*` or `['*']`
  def wildcard: Parser[List[PathInstruction]] =
    (".*" | "['*']") ^^^ List(Wildcard)

  def node: Parser[List[PathInstruction]] =
    wildcard |
      named |
      subscript

  val expression: Parser[List[PathInstruction]] = {
    phrase(root ~> rep(node) ^^ (x => x.flatten))
  }

  def parse(str: String): Option[List[PathInstruction]] = {
    this.parseAll(expression, str) match {
      case Success(result, _) =>
        Some(result)

      case NoSuccess(msg, next) =>
        None
    }
  }
}

private[this] object GetJsonObject {
  private val jsonFactory = new JsonFactory()

  // Enabled for Hive compatibility
  jsonFactory.enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
}

case class GetJsonObject(
    jsonExpression: Expression,
    pathExpression: Expression)
  extends Expression
  with CodegenFallback {

  import GetJsonObject._
  import PathInstruction._
  import WriteStyle._

  override def eval(input: InternalRow): Any = {
    val json = jsonExpression.eval(input).asInstanceOf[UTF8String]
    if (json == null) {
      return null
    }

    pathParser(input) match {
      case Some(path) =>
        try {
          val parser = jsonFactory.createParser(json.getBytes)
          val output = new ByteArrayOutputStream()
          val generator = jsonFactory.createGenerator(output, JsonEncoding.UTF8)
          parser.nextToken()
          val dirty = evaluatePath(parser, generator, RawStyle, path)
          generator.close()
          if (dirty) {
            UTF8String.fromBytes(output.toByteArray)
          } else {
            null
          }
        } catch {
          case _: JsonProcessingException => null
        }

      case None =>
        null
    }
  }

  override def nullable: Boolean = {
    // get_json_object returns null on invalid json
    true
  }

  override def dataType: DataType = {
    StringType
  }

  override def children: Seq[Expression] = {
    Seq(jsonExpression, pathExpression)
  }

  override lazy val resolved: Boolean = {
    childrenResolved &&
      jsonExpression.dataType.isInstanceOf[StringType] &&
      pathExpression.dataType.isInstanceOf[StringType]
  }

  override def prettyName: String = {
    "get_json_object"
  }

  private val pathParser: InternalRow => Option[List[PathInstruction]] = {
    pathExpression match {
      case Literal(path: UTF8String, StringType) =>
        val cached = JsonPathParser.parse(path.toString)
        Function.const(cached)

      case _ =>
        row: InternalRow =>
          pathExpression.eval(row) match {
            case path: UTF8String => JsonPathParser.parse(path.toString)
            case null => None
          }
    }
  }

  /**
   * Evaluate a list of JsonPath instructions, returning a bool that indicates if any leaf nodes
   * have been written to the generator
   */
  private def evaluatePath(
      p: JsonParser,
      g: JsonGenerator,
      style: WriteStyle,
      path: List[PathInstruction]): Boolean = {
    import com.fasterxml.jackson.core.JsonToken._
    (p.getCurrentToken, path) match {
      case (VALUE_STRING, Nil) if style == RawStyle =>
        // there is no array wildcard or slice parent, emit this string without quotes
        if (p.hasTextCharacters) {
          g.writeRaw(p.getTextCharacters, p.getTextOffset, p.getTextLength)
        } else {
          g.writeRaw(p.getText)
        }
        true

      case (START_ARRAY, Nil) if style == FlattenStyle =>
        // flatten this array into the parent
        var dirty = false
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, QuotedStyle, Nil)
        }
        dirty

      case (_, Nil) =>
        // general case: just copy the child tree verbatim
        g.copyCurrentStructure(p)
        true

      case (START_OBJECT, Key :: xs) =>
        var dirty = false
        while (p.nextToken() != END_OBJECT) {
          if (dirty) {
            // once a match has been found we can skip other fields
            p.skipChildren()
          } else {
            dirty = evaluatePath(p, g, style, xs)
          }
        }
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: xs) =>
        // special handling for the non-structure preserving double wildcard behavior in Hive
        val (style, tail) = xs match {
          case Subscript :: Wildcard :: ys => (FlattenStyle, ys)
          case _ => (QuotedStyle, xs)
        }

        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          // wildcards can have multiple matches, continually update the dirty bit
          dirty |= evaluatePath(p, g, style, tail)
        }
        g.writeEndArray()
        dirty

      case (START_ARRAY, Subscript :: Index(idx) :: xs) =>
        def go: Long => Boolean = {
          case _ if p.getCurrentToken == END_ARRAY =>
            // terminate, nothing has been written
            false

          case 0 =>
            // we've reached the desired index
            val dirty = evaluatePath(p, g, style, xs)
            while (p.nextToken() != END_ARRAY) {
              // advance the token stream to the end of the array
              p.skipChildren()
            }
            dirty

          case i if i > 0 =>
            // skip this token and evaluate the next
            p.skipChildren()
            p.nextToken()
            go(i - 1)
        }

        p.nextToken()
        go(idx)

      case (FIELD_NAME, Named(name) :: xs) if p.getCurrentName == name =>
        // exact field match
        p.nextToken()
        evaluatePath(p, g, style, xs)

      case (FIELD_NAME, Wildcard :: xs) =>
        // wildcard field match
        p.nextToken()
        evaluatePath(p, g, style, xs)

      case _ =>
        p.skipChildren()
        false
    }
  }
}
