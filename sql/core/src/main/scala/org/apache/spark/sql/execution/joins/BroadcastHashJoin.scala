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

package org.apache.spark.sql.execution.joins

import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, BindReferences, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, ExpressionCanonicalizer, GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryNode, SQLExecution, SparkPlan}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{InternalAccumulator, TaskContext}

/**
 * :: DeveloperApi ::
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
@DeveloperApi
case class BroadcastHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  // Use lazy so that we won't do broadcast when calling explain but still cache the broadcast value
  // for the same query.
  @transient
  private lazy val broadcastFuture = {
    val numBuildRows = buildSide match {
      case BuildLeft => longMetric("numLeftRows")
      case BuildRight => longMetric("numRightRows")
    }

    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        // Note that we use .execute().collect() because we don't want to convert data to Scala
        // types
        val input: Array[InternalRow] = buildPlan.execute().map { row =>
          numBuildRows += 1
          row.copy()
        }.collect()
        // The following line doesn't run in a job so we cannot track the metric value. However, we
        // have already tracked it in the above lines. So here we can use
        // `SQLMetrics.nullLongMetric` to ignore it.
        val hashed = HashedRelation(
          input.iterator, SQLMetrics.nullLongMetric, buildSideKeyGenerator, input.size)
        sparkContext.broadcast(hashed)
      }
    }(BroadcastHashJoin.broadcastHashJoinExecutionContext)
  }

  protected override def doPrepare(): Unit = {
    broadcastFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numStreamedRows = buildSide match {
      case BuildLeft => longMetric("numRightRows")
      case BuildRight => longMetric("numLeftRows")
    }
    val numOutputRows = longMetric("numOutputRows")

    val broadcastRelation = Await.result(broadcastFuture, timeout)

    streamedPlan.execute().mapPartitions { streamedIter =>
      val hashedRelation = broadcastRelation.value
      hashedRelation match {
        case unsafe: UnsafeHashedRelation =>
          TaskContext.get().internalMetricsToAccumulators(
            InternalAccumulator.PEAK_EXECUTION_MEMORY).add(unsafe.getUnsafeSize)
        case _ =>
      }
      hashJoin(streamedIter, numStreamedRows, hashedRelation, numOutputRows)
    }
  }

  override def supportCodeGen: Boolean = true

  override def produce(ctx: CodeGenContext, parent: SparkPlan): (RDD[InternalRow], String) = {
    calledParent = parent
    streamedPlan.produce(ctx, this)
  }

  override def consume(ctx: CodeGenContext, child: SparkPlan, columns: Seq[GeneratedExpressionCode]): String = {
    val exprs = streamedKeys.map(x =>
      ExpressionCanonicalizer.execute(BindReferences.bindReference(x, streamedPlan.output)))
    val key = GenerateUnsafeProjection.createCode(ctx, exprs)
    val buildRow = ctx.freshName("buildRow")

    val relation = ctx.freshName("relation")
    val broadcastRelation = Await.result(broadcastFuture, timeout)
    val relationIndex = ctx.broadcasts.size
    ctx.broadcasts += broadcastRelation
    val relationType = classOf[UnsafeHashedRelation].getName
    ctx.addMutableState(relationType, relation, s"$relation = ($relationType) objects[$relationIndex];")

    val buildExpr = buildPlan.schema.zipWithIndex.map { case (f, i) =>
      new BoundReference(i, f.dataType, f.nullable)
    }
    ctx.currentVars = null
    ctx.currentRowTerm = buildRow
    val buildColumns = buildExpr.map(_.gen(ctx))
    val output = buildSide match {
      case BuildLeft => buildColumns ++ columns
      case BuildRight => columns ++ buildColumns
    }
    s"""
       | ${key.code}
       |
       | InternalRow $buildRow = $relation.getOne(${key.primitive});
       | if ($buildRow != null) {
       |   ${buildColumns.map(_.code).mkString("\n")}
       |
       |   ${genNext(ctx, output)}
       | }
     """.stripMargin
  }
}

object BroadcastHashJoin {

  private[joins] val broadcastHashJoinExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-hash-join", 128))
}
