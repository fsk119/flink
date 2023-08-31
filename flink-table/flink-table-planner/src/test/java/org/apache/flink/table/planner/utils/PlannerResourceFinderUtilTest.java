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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.PlannerResourceFinderUtil;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.runtime.stream.sql.FunctionITCase;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Find used resources in the {@link Operation}. */
public class PlannerResourceFinderUtilTest extends TableTestBase {

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
    private static URL udfJar1;
    private static URL udfJar2;
    private static URL udfJar3;

    private final JavaStreamTableTestUtil util = javaStreamTestUtil();
    private final Parser parser = util.getPlanner().getParser();
    private final TableEnvironment tableEnvironment = util.getTableEnv();
    private final PlannerResourceFinderUtil finder =
            new PlannerResourceFinderUtilImpl(
                    op -> util.getPlanner().translateToRel(op),
                    uri ->
                            util.getPlanner()
                                    .getFlinkContext()
                                    .getFunctionCatalog()
                                    .getResourceManager()
                                    .getLocalJarResource(uri));

    @BeforeClass
    public static void setup() throws Exception {
        Map<String, String> codes = new HashMap<>();
        codes.put(
                "GeneratedBoolToInt",
                String.format(
                        "public class GeneratedBoolToInt extends %s {}",
                        FunctionITCase.BoolToInt.class.getCanonicalName()));
        codes.put(
                "GeneratedRawMapViewAggregateFunction",
                String.format(
                        "public class GeneratedRawMapViewAggregateFunction extends %s {}",
                        FunctionITCase.RawMapViewAggregateFunction.class.getCanonicalName()));

        udfJar1 =
                Paths.get(
                                UserClassLoaderJarTestUtils.createJarFile(
                                                TMP_FOLDER.getRoot(), "udf1.jar", codes)
                                        .getPath())
                        .toUri()
                        .toURL();

        udfJar2 =
                Paths.get(
                                UserClassLoaderJarTestUtils.createJarFile(
                                                TMP_FOLDER.getRoot(),
                                                "udf2.jar",
                                                Collections.singletonMap(
                                                        "GeneratedStringSplit",
                                                        String.format(
                                                                "public class GeneratedStringSplit extends %s {}",
                                                                JavaUserDefinedTableFunctions
                                                                        .StringSplit.class
                                                                        .getCanonicalName())))
                                        .getPath())
                        .toUri()
                        .toURL();
        udfJar3 =
                Paths.get(
                                UserClassLoaderJarTestUtils.createJarFile(
                                                TMP_FOLDER.getRoot(),
                                                "udf3.jar",
                                                Collections.singletonMap(
                                                        "GeneratedBooleanToEcho",
                                                        String.format(
                                                                "public class GeneratedBooleanToEcho extends %s {}",
                                                                FunctionITCase.BoolEcho.class
                                                                        .getCanonicalName())))
                                        .getPath())
                        .toUri()
                        .toURL();
    }

    @Before
    public void beforeEach() throws Exception {
        FunctionCatalog functionCatalog = util.getPlanner().getFlinkContext().getFunctionCatalog();
        functionCatalog.registerCatalogFunction(
                UnresolvedIdentifier.of("bool2int"),
                "GeneratedBoolToInt",
                Collections.singletonList(new ResourceUri(ResourceType.JAR, udfJar1.getPath())),
                true);
        functionCatalog.registerCatalogFunction(
                UnresolvedIdentifier.of("gen_concat"),
                "GeneratedRawMapViewAggregateFunction",
                Collections.singletonList(new ResourceUri(ResourceType.JAR, udfJar1.getPath())),
                true);
        functionCatalog.registerCatalogFunction(
                UnresolvedIdentifier.of("string_split"),
                "GeneratedStringSplit",
                Collections.singletonList(new ResourceUri(ResourceType.JAR, udfJar2.getPath())),
                true);
        functionCatalog.registerCatalogFunction(
                UnresolvedIdentifier.of("bool_echo"),
                "GeneratedBooleanToEcho",
                Collections.singletonList(new ResourceUri(ResourceType.JAR, udfJar3.getPath())),
                true);

        tableEnvironment.executeSql(
                "CREATE TABLE source ( a INT NOT NULL, b STRING, c BOOLEAN NOT NULL, proctime AS proctime()) WITH ( 'connector' = 'values')");
        tableEnvironment.executeSql(
                "CREATE TABLE dim ( a INT NOT NULL, proctime AS proctime()) WITH ( 'connector' = 'values')");
        tableEnvironment.executeSql("CREATE TABLE sink ( a INT) WITH ( 'connector' = 'values' )");
    }

    @Test
    public void testFindUsedResourceInComputedColumn() {
        tableEnvironment.executeSql(
                "CREATE TABLE source_with_cl (a BOOLEAN NOT NULL, b AS bool2int(a)) WITH ('connector' = 'values')");
        runTest("INSERT INTO sink SELECT CAST(b AS INT) FROM source_with_cl", udfJar1);
    }

    @Test
    public void testFindUsedResourceInProject() {
        runTest(
                "INSERT INTO sink SELECT bool2int(CAST(a AS BOOLEAN)) FROM (VALUES ('true')) AS T(a)",
                udfJar1);
    }

    @Test
    public void testFindUsedResourceInFilter() {
        runTest(
                "INSERT INTO sink "
                        + "SELECT 1 FROM (VALUES ('true')) AS T(a) "
                        + "WHERE bool2int(CAST(a AS BOOLEAN)) > 0",
                udfJar1);
    }

    @Test
    public void testFindUsedResourceInJoin() {
        runTest(
                "INSERT INTO sink "
                        + "SELECT source.a FROM source "
                        + "INNER JOIN dim on bool2int((source.a - dim.a) >= 0) = 0",
                udfJar1);
    }

    @Test
    public void testFindUsedResourceInLateralTable() {
        runTest(
                "INSERT INTO sink "
                        + "SELECT CAST(v AS INT) "
                        + "FROM source, "
                        + "LATERAL TABLE(string_split(b, ',')) as T(v)",
                udfJar2);
    }

    @Test
    public void testFindUsedResourceInLookupJoin() {
        runTest(
                "INSERT INTO sink "
                        + "SELECT source.a FROM source "
                        + "INNER JOIN dim FOR SYSTEM_TIME AS OF source.proctime on bool2int((source.a - dim.a) >= 0) = 0",
                udfJar1);
    }

    @Test
    public void testFindUsedResourceInWindowJoin() {
        runTest(
                "INSERT INTO sink "
                        + "SELECT L.max_a "
                        + "FROM ("
                        + "  SELECT max(a) as max_a, window_start, window_end "
                        + "  FROM TABLE ("
                        + "    TUMBLE(TABLE source, DESCRIPTOR(proctime), INTERVAL '5' SECOND))"
                        + "  GROUP BY window_start, window_end"
                        + ") L "
                        + "INNER JOIN ("
                        + "  SELECT max(a) as max_a, window_start, window_end"
                        + "  FROM TABLE("
                        + "    TUMBLE(TABLE dim, DESCRIPTOR(proctime), INTERVAL '5' SECOND)"
                        + "  )"
                        + "   GROUP BY window_start, window_end"
                        + ") R"
                        + " ON L.window_start = R.window_start AND L.window_end = R.window_end AND bool2int((L.max_a - R.max_a) >= 0) = 0",
                udfJar1);
    }

    @Test
    public void testFindUsedResourceInAggregate() {
        runTest("INSERT INTO sink SELECT CAST(gen_concat(b) AS INT) FROM source", udfJar1);
    }

    @Test
    public void testFindUsedResourceInWindowAggregate() {
        runTest(
                "INSERT INTO sink "
                        + "SELECT CAST(gen_concat(b) AS INT)"
                        + "FROM TABLE("
                        + "   CUMULATE("
                        + "     TABLE source,"
                        + "     DESCRIPTOR(proctime),"
                        + "     INTERVAL '5' SECOND,"
                        + "     INTERVAL '15' SECOND)"
                        + ")",
                udfJar1);
    }

    @Test
    public void testFindUsedResourceInMatch() {
        runTest(
                "INSERT INTO sink\n"
                        + "SELECT T.bc\n"
                        + "FROM source\n"
                        + "MATCH_RECOGNIZE (\n"
                        + "  ORDER BY proctime\n"
                        + "  MEASURES\n"
                        + "    A.a AS aid,\n"
                        + "    bool2int(B.c) AS bc,\n"
                        + "    MATCH_ROWTIME() as matchRowtime,\n"
                        + "    C.b AS cb\n"
                        + "  PATTERN (A B C)\n"
                        + "  DEFINE\n"
                        + "    A AS a = 1 AND bool_echo(c),\n"
                        + "    B AS a = 2,\n"
                        + "    C AS a = 3\n"
                        + ") AS T",
                udfJar1,
                udfJar3);
    }

    @Test
    public void testFindUsedResourceInDelete() {}

    @Test
    public void testFindUsedMultipleResources() {}

    private void runTest(String sql, URL... usedJars) {
        ModifyOperation operation = (ModifyOperation) parser.parse(sql).get(0);
        assertThat(finder.findResources(operation))
                .isEqualTo(new HashSet<>(Arrays.asList(usedJars)));
    }
}
