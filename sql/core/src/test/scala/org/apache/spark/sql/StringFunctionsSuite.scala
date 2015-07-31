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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.Decimal


class StringFunctionsSuite extends QueryTest {

  private lazy val ctx = org.apache.spark.sql.test.TestSQLContext
  import ctx.implicits._

  test("string concat") {
    val df = Seq[(String, String, String)](("a", "b", null)).toDF("a", "b", "c")

    checkAnswer(
      df.select(concat($"a", $"b"), concat($"a", $"b", $"c")),
      Row("ab", null))

    checkAnswer(
      df.selectExpr("concat(a, b)", "concat(a, b, c)"),
      Row("ab", null))
  }

  test("string concat_ws") {
    val df = Seq[(String, String, String)](("a", "b", null)).toDF("a", "b", "c")

    checkAnswer(
      df.select(concat_ws("||", $"a", $"b", $"c")),
      Row("a||b"))

    checkAnswer(
      df.selectExpr("concat_ws('||', a, b, c)"),
      Row("a||b"))
  }

  test("string Levenshtein distance") {
    val df = Seq(("kitten", "sitting"), ("frog", "fog")).toDF("l", "r")
    checkAnswer(df.select(levenshtein($"l", $"r")), Seq(Row(3), Row(1)))
    checkAnswer(df.selectExpr("levenshtein(l, r)"), Seq(Row(3), Row(1)))
  }

  test("string regex_replace / regex_extract") {
    val df = Seq(
      ("100-200", "(\\d+)-(\\d+)", "300"),
      ("100-200", "(\\d+)-(\\d+)", "400"),
      ("100-200", "(\\d+)", "400")).toDF("a", "b", "c")

    checkAnswer(
      df.select(
        regexp_replace($"a", "(\\d+)", "num"),
        regexp_extract($"a", "(\\d+)-(\\d+)", 1)),
      Row("num-num", "100") :: Row("num-num", "100") :: Row("num-num", "100") :: Nil)

    // for testing the mutable state of the expression in code gen.
    // This is a hack way to enable the codegen, thus the codegen is enable by default,
    // it will still use the interpretProjection if projection followed by a LocalRelation,
    // hence we add a filter operator.
    // See the optimizer rule `ConvertToLocalRelation`
    checkAnswer(
      df.filter("isnotnull(a)").selectExpr(
        "regexp_replace(a, b, c)",
        "regexp_extract(a, b, 1)"),
      Row("300", "100") :: Row("400", "100") :: Row("400-400", "100") :: Nil)
  }

  test("string ascii function") {
    val df = Seq(("abc", "")).toDF("a", "b")
    checkAnswer(
      df.select(ascii($"a"), ascii($"b")),
      Row(97, 0))

    checkAnswer(
      df.selectExpr("ascii(a)", "ascii(b)"),
      Row(97, 0))
  }

  test("string base64/unbase64 function") {
    val bytes = Array[Byte](1, 2, 3, 4)
    val df = Seq((bytes, "AQIDBA==")).toDF("a", "b")
    checkAnswer(
      df.select(base64($"a"), unbase64($"b")),
      Row("AQIDBA==", bytes))

    checkAnswer(
      df.selectExpr("base64(a)", "unbase64(b)"),
      Row("AQIDBA==", bytes))
  }

  test("string encode/decode function") {
    val bytes = Array[Byte](-27, -92, -89, -27, -115, -125, -28, -72, -106, -25, -107, -116)
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    val df = Seq(("大千世界", "utf-8", bytes)).toDF("a", "b", "c")
    checkAnswer(
      df.select(encode($"a", "utf-8"), decode($"c", "utf-8")),
      Row(bytes, "大千世界"))

    checkAnswer(
      df.selectExpr("encode(a, 'utf-8')", "decode(c, 'utf-8')"),
      Row(bytes, "大千世界"))
    // scalastyle:on
  }

  test("string trim functions") {
    val df = Seq(("  example  ", "")).toDF("a", "b")

    checkAnswer(
      df.select(ltrim($"a"), rtrim($"a"), trim($"a")),
      Row("example  ", "  example", "example"))

    checkAnswer(
      df.selectExpr("ltrim(a)", "rtrim(a)", "trim(a)"),
      Row("example  ", "  example", "example"))
  }

  test("string formatString function") {
    val df = Seq(("aa%d%s", 123, "cc")).toDF("a", "b", "c")

    checkAnswer(
      df.select(format_string("aa%d%s", $"b", $"c")),
      Row("aa123cc"))

    checkAnswer(
      df.selectExpr("printf(a, b, c)"),
      Row("aa123cc"))
  }

  test("string instr function") {
    val df = Seq(("aaads", "aa", "zz")).toDF("a", "b", "c")

    checkAnswer(
      df.select(instr($"a", "aa")),
      Row(1))

    checkAnswer(
      df.selectExpr("instr(a, b)"),
      Row(1))
  }

  test("string substring_index function") {
    val df = Seq(("www.apache.org", ".", "zz")).toDF("a", "b", "c")
    checkAnswer(
      df.select(substring_index($"a", ".", 3)),
      Row("www.apache.org"))
    checkAnswer(
      df.select(substring_index($"a", ".", 2)),
      Row("www.apache"))
    checkAnswer(
      df.select(substring_index($"a", ".", 1)),
      Row("www"))
    checkAnswer(
      df.select(substring_index($"a", ".", 0)),
      Row(""))
    checkAnswer(
      df.select(substring_index(lit("www.apache.org"), ".", -1)),
      Row("org"))
    checkAnswer(
      df.select(substring_index(lit("www.apache.org"), ".", -2)),
      Row("apache.org"))
    checkAnswer(
      df.select(substring_index(lit("www.apache.org"), ".", -3)),
      Row("www.apache.org"))
    // str is empty string
    checkAnswer(
      df.select(substring_index(lit(""), ".", 1)),
      Row(""))
    // empty string delim
    checkAnswer(
      df.select(substring_index(lit("www.apache.org"), "", 1)),
      Row(""))
    // delim does not exist in str
    checkAnswer(
      df.select(substring_index(lit("www.apache.org"), "#", 1)),
      Row("www.apache.org"))
    // delim is 2 chars
    checkAnswer(
      df.select(substring_index(lit("www||apache||org"), "||", 2)),
      Row("www||apache"))
    checkAnswer(
      df.select(substring_index(lit("www||apache||org"), "||", -2)),
      Row("apache||org"))
    // null
    checkAnswer(
      df.select(substring_index(lit(null), "||", 2)),
      Row(null))
    checkAnswer(
      df.select(substring_index(lit("www.apache.org"), null, 2)),
      Row(null))
    // non ascii chars
    // scalastyle:off
    checkAnswer(
      df.selectExpr("""substring_index("大千世界大千世界", "千", 2)"""),
      Row("大千世界大"))
    // scalastyle:on
  }

  test("string locate function") {
    val df = Seq(("aaads", "aa", "zz", 1)).toDF("a", "b", "c", "d")

    checkAnswer(
      df.select(locate("aa", $"a"), locate("aa", $"a", 1)),
      Row(1, 2))

    checkAnswer(
      df.selectExpr("locate(b, a)", "locate(b, a, d)"),
      Row(1, 2))
  }

  test("string padding functions") {
    val df = Seq(("hi", 5, "??")).toDF("a", "b", "c")

    checkAnswer(
      df.select(lpad($"a", 1, "c"), lpad($"a", 5, "??"), rpad($"a", 1, "c"), rpad($"a", 5, "??")),
      Row("h", "???hi", "h", "hi???"))

    checkAnswer(
      df.selectExpr("lpad(a, b, c)", "rpad(a, b, c)", "lpad(a, 1, c)", "rpad(a, 1, c)"),
      Row("???hi", "hi???", "h", "h"))
  }

  test("string repeat function") {
    val df = Seq(("hi", 2)).toDF("a", "b")

    checkAnswer(
      df.select(repeat($"a", 2)),
      Row("hihi"))

    checkAnswer(
      df.selectExpr("repeat(a, 2)", "repeat(a, b)"),
      Row("hihi", "hihi"))
  }

  test("string reverse function") {
    val df = Seq(("hi", "hhhi")).toDF("a", "b")

    checkAnswer(
      df.select(reverse($"a"), reverse($"b")),
      Row("ih", "ihhh"))

    checkAnswer(
      df.selectExpr("reverse(b)"),
      Row("ihhh"))
  }

  test("string space function") {
    val df = Seq((2, 3)).toDF("a", "b")

    checkAnswer(
      df.selectExpr("space(b)"),
      Row("   "))
  }

  test("string split function") {
    val df = Seq(("aa2bb3cc", "[1-9]+")).toDF("a", "b")

    checkAnswer(
      df.select(split($"a", "[1-9]+")),
      Row(Seq("aa", "bb", "cc")))

    checkAnswer(
      df.selectExpr("split(a, '[1-9]+')"),
      Row(Seq("aa", "bb", "cc")))
  }

  test("string / binary length function") {
    val df = Seq(("123", Array[Byte](1, 2, 3, 4), 123)).toDF("a", "b", "c")
    checkAnswer(
      df.select(length($"a"), length($"b")),
      Row(3, 4))

    checkAnswer(
      df.selectExpr("length(a)", "length(b)"),
      Row(3, 4))

    intercept[AnalysisException] {
      checkAnswer(
        df.selectExpr("length(c)"), // int type of the argument is unacceptable
        Row("5.0000"))
    }
  }

  test("number format function") {
    val tuple =
      ("aa", 1.asInstanceOf[Byte], 2.asInstanceOf[Short],
        3.13223f, 4, 5L, 6.48173d, Decimal(7.128381))
    val df =
      Seq(tuple)
        .toDF(
          "a", // string "aa"
          "b", // byte    1
          "c", // short   2
          "d", // float   3.13223f
          "e", // integer 4
          "f", // long    5L
          "g", // double  6.48173d
          "h") // decimal 7.128381

    checkAnswer(
      df.select(format_number($"f", 4)),
      Row("5.0000"))

    checkAnswer(
      df.selectExpr("format_number(b, e)"), // convert the 1st argument to integer
      Row("1.0000"))

    checkAnswer(
      df.selectExpr("format_number(c, e)"), // convert the 1st argument to integer
      Row("2.0000"))

    checkAnswer(
      df.selectExpr("format_number(d, e)"), // convert the 1st argument to double
      Row("3.1322"))

    checkAnswer(
      df.selectExpr("format_number(e, e)"), // not convert anything
      Row("4.0000"))

    checkAnswer(
      df.selectExpr("format_number(f, e)"), // not convert anything
      Row("5.0000"))

    checkAnswer(
      df.selectExpr("format_number(g, e)"), // not convert anything
      Row("6.4817"))

    checkAnswer(
      df.selectExpr("format_number(h, e)"), // not convert anything
      Row("7.1284"))

    intercept[AnalysisException] {
      checkAnswer(
        df.selectExpr("format_number(a, e)"), // string type of the 1st argument is unacceptable
        Row("5.0000"))
    }

    intercept[AnalysisException] {
      checkAnswer(
        df.selectExpr("format_number(e, g)"), // decimal type of the 2nd argument is unacceptable
        Row("5.0000"))
    }

    // for testing the mutable state of the expression in code gen.
    // This is a hack way to enable the codegen, thus the codegen is enable by default,
    // it will still use the interpretProjection if projection follows by a LocalRelation,
    // hence we add a filter operator.
    // See the optimizer rule `ConvertToLocalRelation`
    val df2 = Seq((5L, 4), (4L, 3), (3L, 2)).toDF("a", "b")
    checkAnswer(
      df2.filter("b>0").selectExpr("format_number(a, b)"),
      Row("5.0000") :: Row("4.000") :: Row("3.00") :: Nil)
  }
}
