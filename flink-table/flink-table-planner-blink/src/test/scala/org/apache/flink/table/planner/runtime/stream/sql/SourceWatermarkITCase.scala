/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.sql

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.flink.api.common.eventtime.Watermark
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.rules.logical.PushWatermarkIntoTableSourceScanRuleTest
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class SourceWatermarkITCase extends StreamingTestBase{
  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1)
  }

  def testWatermarkTemp(
      data: Seq[Row],
      cols: String,
      expectedWatermarkOutput: Seq[LocalDateTime],
      expectedData: Seq[String]): Unit = {
    val dataId = TestValuesTableFactory.registerData(data)
    val ddl =
      s"""
        | create table MyTable (
        |   $cols
        | ) with (
        |   'connector' = 'values',
        |   'bounded' = 'false',
        |   'runtime-source' = 'RecordWatermarkSourceFunction',
        |   'data-id' = '$dataId'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl)

    val query = "SELECT * FROM MyTable"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val actualWatermark = TestValuesTableFactory.getWatermarkOutput
      .asScala
      .map(x => TimestampData.fromEpochMillis(x.getTimestamp).toLocalDateTime)

    assertEquals(expectedWatermarkOutput, actualWatermark)
    assertEquals(expectedData.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimpleWatermark(): Unit = {
    val data = Seq(
      row(1, 2L, Timestamp.valueOf("2020-11-21 19:00:05.23").toLocalDateTime),
      row(1, 2L, Timestamp.valueOf("2020-11-21 21:00:05.23").toLocalDateTime)
    )

    val cols =
      s"""
         |  a int,
         |  b bigint,
         |  c timestamp(3),
         |  watermark for c as c - interval '5' second
         |""".stripMargin

    val expectedWatermarkOutput = Seq(Timestamp.valueOf("2020-11-21 19:00:00.23").toLocalDateTime,
      Timestamp.valueOf("2020-11-21 21:00:00.23").toLocalDateTime)
    val expectedData = Seq(
      "1,2,2020-11-21T19:00:05.230",
      "1,2,2020-11-21T21:00:05.230")

    testWatermarkTemp(data, cols, expectedWatermarkOutput, expectedData)
  }

  @Test
  def testSimpleTransposeNotNull(): Unit = {
    val data = Seq(
      row(1, 2L, Timestamp.valueOf("2020-11-21 19:00:05.23").toLocalDateTime),
      row(1, 2L, Timestamp.valueOf("2020-11-21 21:00:05.23").toLocalDateTime)
    )

    val cols =
      s"""
        |  a int,
        |  b bigint,
        |  c timestamp(3) not null,
        |  d as c + interval '5' second,
        |  watermark for d as d - interval '5' second
        |""".stripMargin

    val expectedWatermarkOutput = Seq(Timestamp.valueOf("2020-11-21 19:00:05.23").toLocalDateTime,
      Timestamp.valueOf("2020-11-21 21:00:05.23").toLocalDateTime)
    val expectedData = Seq(
      "1,2,2020-11-21T19:00:05.230,2020-11-21T19:00:10.230",
      "1,2,2020-11-21T21:00:05.230,2020-11-21T21:00:10.230")

    testWatermarkTemp(data, cols, expectedWatermarkOutput, expectedData)
  }

  @Test
  def testTransposeWithNestedRow(): Unit = {
    val data = Seq(
      row(1, 2L, row("i1", row("i2", Timestamp.valueOf("2020-11-21 19:00:05.23").toLocalDateTime))),
      row(2, 3L, row("j1", row("j2", Timestamp.valueOf("2020-11-21 21:00:05.23").toLocalDateTime)))
    )

    val cols =
      """
        |  a int,
        |  b bigint,
        |  c row<name string, d row<e string, f timestamp(3)>>,
        |  g as c.d.f,
        |  watermark for g as g - interval '5' second
        |""".stripMargin

    val expectedWatermarkOutput = Seq(Timestamp.valueOf("2020-11-21 19:00:00.23").toLocalDateTime,
      Timestamp.valueOf("2020-11-21 21:00:00.23").toLocalDateTime)
    val expectedData = Seq(
      "1,2,i1,i2,2020-11-21T19:00:05.230,2020-11-21T19:00:05.230",
      "2,3,j1,j2,2020-11-21T21:00:05.230,2020-11-21T21:00:05.230"
    )

    testWatermarkTemp(data, cols, expectedWatermarkOutput, expectedData)
  }

  @Test
  def testTransposeWithUdf(): Unit = {
    tEnv.createTemporarySystemFunction(
      "func1", new PushWatermarkIntoTableSourceScanRuleTest.InnerUdf)
    val data = Seq(
      row(1, 2L, Timestamp.valueOf("2020-11-21 19:00:05.23").toLocalDateTime),
      row(1, 2L, Timestamp.valueOf("2020-11-21 21:00:05.23").toLocalDateTime)
    )

    val cols =
      """
        |  a int,
        |  b bigint,
        |  c timestamp(3),
        |  d as func1(c),
        |  watermark for d as d - interval '5' second
        |""".stripMargin

    val expected = Seq(
      Timestamp.valueOf("2020-11-21 19:00:05.23").toLocalDateTime,
      Timestamp.valueOf("2020-11-21 21:00:05.23").toLocalDateTime
    )
    val expectedData = Seq(
      "1,2,2020-11-21T19:00:05.230,2020-11-21T19:00:10.230",
      "1,2,2020-11-21T21:00:05.230,2020-11-21T21:00:10.230"
    )

    testWatermarkTemp(data, cols, expected, expectedData)
  }
}
