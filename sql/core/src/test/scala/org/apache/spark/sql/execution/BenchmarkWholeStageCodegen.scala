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

package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Benchmark
import org.apache.spark.{SparkFunSuite, SparkConf, SparkContext}

/**
  * Benchmark to measure whole stage codegen performance.
  * To run this:
  *  build/sbt sql/test-only BenchmarkWholeStageCodegen
  */
class BenchmarkWholeStageCodegen extends SparkFunSuite {
  val conf = new SparkConf()
  val sc = new SparkContext("local[1]", "test-sql-context", conf)
  val sqlContext = new SQLContext(sc)

  def intScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Single Int Column Scan", values)

    benchmark.addCase("Without whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).filter("(id & 1) = 1").count()
    }

    benchmark.addCase("With whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).filter("(id & 1) = 1").count()
    }

    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      Single Int Column Scan:      Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------
      Without whole stage codegen       3465.17            30.26         1.00 X
      With whole stage codegen          1388.39            75.52         2.50 X
    */
    benchmark.run()
  }

  ignore("benchmark") {
    intScanBenchmark(1024 * 1024 * 100)
  }
}
