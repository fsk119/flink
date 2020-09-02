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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.planner.plan.rules.logical.PushWatermarkIntoTableSourceScanRuleTest

import org.junit.Test

class SourceWatermarkTest extends TableTestBase {

  private val util = streamTestUtil()

  @Test
  def testSimpleWatermark(): Unit = {
    val ddl =
      """
        | create table MyTable (
        |   a int,
        |   b bigint,
        |   c timestamp(3),
        |   watermark for c as c - interval '5' second
        | ) with (
        |   'connector' = 'values',
        |   'bounded' = 'false'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl)
    util.verifyPlan("select * from MyTable")
  }

  @Test
  def testSimpleTranspose(): Unit = {
    val ddl =
      """
        | create table MyTable (
        |   a int,
        |   b bigint,
        |   c timestamp(3),
        |   d as c + interval '5' second,
        |   watermark for d as d - interval '5' second
        | ) with (
        |   'connector' = 'values',
        |   'bounded' = 'false'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl)
    util.verifyPlan("select * from MyTable")
  }

  @Test
  def testSimpleTransposeNotNull(): Unit = {
    val ddl =
      """
        | create table MyTable (
        |   a int,
        |   b bigint,
        |   c timestamp(3) not null,
        |   d as c + interval '5' second,
        |   watermark for d as d - interval '5' second
        | ) with (
        |   'connector' = 'values',
        |   'bounded' = 'false'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl)
    util.verifyPlan("select * from MyTable")
  }

  @Test
  def testComputedColumnWithMultipleInputs(): Unit = {
    val ddl =
      """
        | create table MyTable (
        |   a string,
        |   b string,
        |   c as to_timestamp(a, b),
        |   watermark for c as c - interval '5' second
        | ) with (
        |   'connector' = 'values',
        |   'bounded' = 'false'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl)
    util.verifyPlan("select * from MyTable")
  }

  @Test
  def testTransposeWithRow(): Unit = {
    val ddl =
      """
        | create table MyTable (
        |   a int,
        |   b bigint,
        |   c row<name string, d timestamp(3)>,
        |   d as c.d,
        |   watermark for d as d - interval '5' second
        | ) with (
        |   'connector' = 'values',
        |   'bounded' = 'false'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl)
    util.verifyPlan("select * from MyTable")
  }

  @Test
  def testTransposeWithNestedRow(): Unit = {
    val ddl =
      """
        | create table MyTable (
        |   a int,
        |   b bigint,
        |   c row<name string, d row<e string, f timestamp(3)>>,
        |   g as c.d.f,
        |   watermark for g as g - interval '5' second
        | ) with (
        |   'connector' = 'values',
        |   'bounded' = 'false'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl)
    util.verifyPlan("select * from MyTable")
  }

  @Test
  def testTransposeWithUdf(): Unit = {
    util.addFunction("func1", new PushWatermarkIntoTableSourceScanRuleTest.InnerUdf)
    val ddl =
      """
        | create table MyTable (
        |   a int,
        |   b bigint,
        |   c timestamp(3),
        |   d as func1(c),
        |   watermark for d as d - interval '5' second
        | ) with (
        |   'connector' = 'values',
        |   'bounded' = 'false'
        | )
        |""".stripMargin
    util.tableEnv.executeSql(ddl)
    util.verifyPlan("select * from MyTable")
  }
}
