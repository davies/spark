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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.{Row, SQLContext}

class ParquetThriftCompatibilitySuite extends ParquetCompatibilityTest {
  import ParquetCompatibilityTest._

  override val sqlContext: SQLContext = TestSQLContext

  private val parquetFilePath =
    Thread.currentThread().getContextClassLoader.getResource("parquet-thrift-compat.snappy.parquet")

  test("Read Parquet file generated by parquet-thrift") {
    logInfo(
      s"""Schema of the Parquet file written by parquet-thrift:
         |${readParquetSchema(parquetFilePath.toString)}
       """.stripMargin)

    checkAnswer(sqlContext.read.parquet(parquetFilePath.toString), (0 until 10).map { i =>
      def nullable[T <: AnyRef]: ( => T) => T = makeNullable[T](i)

      val suits = Array("SPADES", "HEARTS", "DIAMONDS", "CLUBS")

      Row(
        i % 2 == 0,
        i.toByte,
        (i + 1).toShort,
        i + 2,
        i.toLong * 10,
        i.toDouble + 0.2d,
        // Thrift `BINARY` values are actually unencoded `STRING` values, and thus are always
        // treated as `BINARY (UTF8)` in parquet-thrift, since parquet-thrift always assume
        // Thrift `STRING`s are encoded using UTF-8.
        s"val_$i",
        s"val_$i",
        // Thrift ENUM values are converted to Parquet binaries containing UTF-8 strings
        suits(i % 4),

        nullable(i % 2 == 0: java.lang.Boolean),
        nullable(i.toByte: java.lang.Byte),
        nullable((i + 1).toShort: java.lang.Short),
        nullable(i + 2: Integer),
        nullable((i * 10).toLong: java.lang.Long),
        nullable(i.toDouble + 0.2d: java.lang.Double),
        nullable(s"val_$i"),
        nullable(s"val_$i"),
        nullable(suits(i % 4)),

        Seq.tabulate(3)(n => s"arr_${i + n}"),
        // Thrift `SET`s are converted to Parquet `LIST`s
        Seq(i),
        Seq.tabulate(3)(n => (i + n: Integer) -> s"val_${i + n}").toMap,
        Seq.tabulate(3) { n =>
          (i + n) -> Seq.tabulate(3) { m =>
            Row(Seq.tabulate(3)(j => i + j + m), s"val_${i + m}")
          }
        }.toMap)
    })
  }
}
