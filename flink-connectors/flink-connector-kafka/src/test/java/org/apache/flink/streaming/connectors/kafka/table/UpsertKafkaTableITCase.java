package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBaseWithFlink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/**
 * Upsert-kafka IT cases.
 */
@RunWith(Parameterized.class)
public class UpsertKafkaTableITCase extends KafkaTestBaseWithFlink {

	private static final String JSON_FORMAT = "json";
	private static final String CSV_FORMAT = "csv";

	@Parameterized.Parameter
	public String format;

	@Parameterized.Parameters(name = "format = {0}")
	public static Object[] parameters() {
		return new Object[]{
			JSON_FORMAT
		};
	}

	protected StreamExecutionEnvironment env;
	protected StreamTableEnvironment tEnv;

	private static final String USERS_TOPIC = "users";
	private static final String ENRICHED_PAGE_VIEW_TOPIC = "enriched_page_view";
	private static final String PAGEVIEWS_PER_REGION_5_MIN_TOPIC = "pageviews_per_region_5min";
	private static final String PAGEVIEWS_PER_REGION_TOPIC = "pageviews_per_region";

	@Before
	public void setup() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		tEnv = StreamTableEnvironment.create(
				env,
				EnvironmentSettings.newInstance()
						// Watermark is only supported in blink planner
						.useBlinkPlanner()
						.inStreamingMode()
						.build()
		);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		// we have to use single parallelism,
		// because we will count the messages in sink to terminate the job
		env.setParallelism(1);
	}

	@Test
	public void testPipeline() throws Exception {
		createReferenceTableAndRegisterData();
//		dimensionJoinAndRegisterData();
//		createAggregatedTableAndRegisterData();
//		createFinalTopicAndRegisterData();
		// ------------- cleanup -------------------
		deleteTestTopic(USERS_TOPIC + "_" + format);
//		deleteTestTopic(ENRICHED_PAGE_VIEW_TOPIC + "_" + format);
//		deleteTestTopic(PAGEVIEWS_PER_REGION_5_MIN_TOPIC + "_" + format);
//		deleteTestTopic(PAGEVIEWS_PER_REGION_TOPIC + "_" + format);
	}

	private void createReferenceTableAndRegisterData() throws Exception {
		final String topic = USERS_TOPIC + "_" + format;
		createTopic(topic, true);
		String bootstraps = standardProps.getProperty("bootstrap.servers");
		// ------------- create table ---------------
		final String createTable = String.format(
				"CREATE TABLE users (\n"
						+ "  `user_id` BIGINT,\n"
						+ "  `user_name` STRING,\n"
						+ "  `region` STRING,\n"
						+ "PRIMARY KEY (`user_id`) NOT ENFORCED\n"
						+ ") WITH (\n"
						+ "  'connector' = 'upsert-kafka',\n"
						+ "  'topic' = '%s',\n"
						+ "  'properties.bootstrap.servers' = '%s',\n"
						+ "  %s\n"
						+ ")",
				topic,
				bootstraps,
				formatOptions());
		tEnv.executeSql(createTable);
		String initialValues = "INSERT INTO users " +
				"VALUES (100, 'Bob', 'Beijing')," +
				"(101, 'Alice', 'Shanghai')," +
				"(102, 'Greg', 'Berlin')," +
				"(103, 'Richard', 'Berlin')," +
				"(101, 'Alice', 'Shanghai')," +
				"(101, 'Alice', 'Hangzhou')," +
				"(103, CAST(NULL AS STRING), CAST(NULL AS STRING))";
		tEnv.executeSql(initialValues).await();

		// ---------- consume stream from sink -------------------
		// get results from the append-only format
		final List<Row> result = KafkaTableTestUtils.collectRows(tEnv.sqlQuery("SELECT * FROM UpsertKafka"), 3);
		final Set<String> expected = new HashSet<>();
		assertEquals("Unmatched size between expected results and actual results", expected.size(), result.size());

		for (Row actualRow: result) {
			if (!expected.contains(actualRow.toString())) {
				Assert.fail(String.format("The actual row %s doesn't exist in the expected row.", actualRow.toString()));
			}
		}
	}

	private void dimensionJoinAndRegisterData() throws Exception {
		final String topic = ENRICHED_PAGE_VIEW_TOPIC + "_" + format;
		createTopic(topic, false);
		String bootstraps = standardProps.getProperty("bootstrap.servers");

		// create fact table
		Table pageView = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
				Row.of(100, 10001, Timestamp.valueOf("2020-10-01 08:01:00")),
				Row.of(102, 10002, Timestamp.valueOf("2020-10-01 08:02:00")),
				Row.of(101, 10002, Timestamp.valueOf("2020-10-01 08:04:00")),
				Row.of(102, 10004, Timestamp.valueOf("2020-10-01 08:06:00")),
				Row.of(102, 10003, Timestamp.valueOf("2020-10-01 08:07:00"))
		)),
				$("user_id"), $("page_id"), $("viewtime"), $("proctime").proctime());
		tEnv.createTemporaryView("pageView", pageView);

		// ------------- create table ---------------
		final String createRefTable =
				String.format(
						"CREATE TABLE users (\n"
								+ "  `user_id` BIGINT,\n"
								+ "  `user_name` STRING,\n"
								+ "  `region` STRING,\n"
								+ "PRIMARY KEY (`user_id`)\n"
								+ ") WITH (\n"
								+ "  'connector' = 'upsert-kafka',\n"
								+ "  'topic' = '%s',\n"
								+ "  'properties.bootstrap.servers' = '%s',\n"
								+ "  %s\n"
								+ ")",
						topic,
						bootstraps,
						formatOptions()
				);
		tEnv.executeSql(createRefTable);
		final String createSinkTable = String.format(
				"CREATE TABLE pageviews_enriched (\n"
						+ "  `user_id` BIGINT,\n"
						+ "  `page_id` BIGINT,\n"
						+ "  `viewtime` TIMESTAMP,\n"
						+ "  `user_region` STRING\n"
						+ ") WITH (\n"
						+ "  'connector' = 'kafka',\n"
						+ "  'topic' = '%s',\n"
						+ "  'properties.bootstrap.servers' = '%s',\n"
						+ "  'value.fields' = 'user_id;page_id;view',\n"
						+ "  %s\n"
						+ ")",
				topic,
				bootstraps,
				formatOptions());
		tEnv.executeSql(createSinkTable);
		// join two streams
		tEnv.executeSql(
				"INSERT INTO pageviews_enriched " +
						"SELECT *" +
						"FROM pageview AS p" +
						"LEFT JOIN users FOR SYSTEM_TIME AS OF p.proctime AS u" +
						"ON p.user_id = u.user_id").await();

		// ---------- Consume stream from sink -------------------
		final List<Row> result = KafkaTableTestUtils.collectRows(tEnv.sqlQuery("SELECT * FROM pageviews_enriched"), 3);
		final Set<String> expected = new HashSet<>();
		assertEquals("Unmatched size between expected results and actual results", expected.size(), result.size());

		for (Row actualRow: result) {
			if (!expected.contains(actualRow.toString())) {
				Assert.fail(String.format("The actual row %s doesn't exist in the expected row.", actualRow.toString()));
			}
		}
	}

	private void createAggregatedTableAndRegisterData() throws Exception {
		final String sinkTopic = PAGEVIEWS_PER_REGION_5_MIN_TOPIC + "_" + format;
		createTopic(sinkTopic, true);
		String bootstraps = standardProps.getProperty("bootstrap.servers");
		final String createSourceTable = String.format(
				"CREATE TABLE pageviews_enriched (\n"
						+ "  `user_id` BIGINT,\n"
						+ "  `page_id` BIGINT,\n"
						+ "  `viewtime` TIMESTAMP,\n"
						+ "  `user_region` STRING\n"
						+ ") WITH (\n"
						+ "  'connector' = 'kafka',\n"
						+ "  'topic' = '%s',\n"
						+ "  'properties.bootstrap.servers' = '%s',\n"
						+ "  'value.fields' = 'user_id;page_id;viewtime;user_region',\n"
						+ "  %s\n"
						+ ")",
				ENRICHED_PAGE_VIEW_TOPIC + "_" + format,
				bootstraps,
				formatOptions());
		tEnv.executeSql(createSourceTable);
		final String createSinkTable = String.format(
				"CREATE TABLE pageviews_per_region_5min (\n"
						+ "  `window_start` STRING,\n"
						+ "  `region` STRING,\n"
						+ "  `view_count` BIGINT,\n"
						+ "PRIMARY KEY (`window_start`, `region`) NOT ENFORCED\n"
						+ ") WITH (\n"
						+ "  'connector' = 'upsert-kafka',\n"
						+ "  'topic' = '%s',\n"
						+ "  'properties.bootstrap.servers' = '%s',\n"
						+ "  %s\n"
						+ ")",
				sinkTopic,
				bootstraps,
				formatOptions());
		tEnv.executeSql(createSinkTable);
		tEnv.executeSql(
				"INSERT INTO pageviews_per_region_5min\n" +
						"SELECT\n" +
						"  TUMBLE_START(viewtime, INTERVAL '5' MINUTE),\n" +
						"  region,\n" +
						"  COUNT(*)\n" +
						"FROM pageviews_enriched\n" +
						"GROUP BY region, TUMBLE(viewtime, INTERVAL '5' MINUTE)");

		// ---------- Consume stream from sink -------------------
		final List<Row> result = KafkaTableTestUtils.collectRows(tEnv.sqlQuery("SELECT * FROM pageviews_per_region_5min"), 3);
		final Set<String> expected = new HashSet<>();
		assertEquals("Unmatched size between expected results and actual results", expected.size(), result.size());

		for (Row actualRow: result) {
			if (!expected.contains(actualRow.toString())) {
				Assert.fail(String.format("The actual row %s doesn't exist in the expected row.", actualRow.toString()));
			}
		}
	}

	private void createFinalTopicAndRegisterData() throws Exception {
		final String sinkTopic = PAGEVIEWS_PER_REGION_TOPIC + "_" + format;
		createTopic(sinkTopic, true);
		String bootstraps = standardProps.getProperty("bootstrap.servers");

		final String createSourceTable = String.format(
				"CREATE TABLE pageviews_per_region_5min (\n"
						+ "  `window_start` STRING,\n"
						+ "  `region` STRING,\n"
						+ "  `view_count` BIGINT,\n"
						+ "PRIMARY KEY (`window_start`, `region`) NOT ENFORCED\n"
						+ ") WITH (\n"
						+ "  'connector' = 'upsert-kafka',\n"
						+ "  'topic' = '%s',\n"
						+ "  'properties.bootstrap.servers' = '%s',\n"
						+ "  %s\n"
						+ ")",
				PAGEVIEWS_PER_REGION_5_MIN_TOPIC + "_" + format,
				bootstraps,
				formatOptions());
		tEnv.executeSql(createSourceTable);
		final String createSinkTable = String.format(
				"CREATE TABLE pageviews_per_region (\n"
						+ "  `region` STRING,\n"
						+ "  `view_count` BIGINT,\n"
						+ "PRIMARY KEY (`region`) NOT ENFORCED\n"
						+ ") WITH (\n"
						+ "  'connector' = 'upsert-kafka',\n"
						+ "  'topic' = '%s',\n"
						+ "  'properties.bootstrap.servers' = '%s',\n"
						+ "  %s\n"
						+ ")",
				sinkTopic,
				bootstraps,
				formatOptions()
		);
		tEnv.executeSql(createSinkTable);
		tEnv.executeSql(
				"INSERT INTO pageviews_per_region"
				+ "SELECT\n"
				+ "  region,\n"
				+ "  SUM(view_count),\n"
				+ "FROM pageviews_per_region_5min\n"
				+ "GROUP BY region"
		);
		// ---------- Consume stream from sink -------------------
		final List<Row> result = KafkaTableTestUtils.collectRows(tEnv.sqlQuery("SELECT * FROM pageviews_per_region"), 3);
		final Set<String> expected = new HashSet<>();
		assertEquals("Unmatched size between expected results and actual results", expected.size(), result.size());

		for (Row actualRow: result) {
			if (!expected.contains(actualRow.toString())) {
				Assert.fail(String.format("The actual row %s doesn't exist in the expected row.", actualRow.toString()));
			}
		}
	}

	private void createTopic(String topic, boolean enableCompacted) {
		Properties props = new Properties();
		if (enableCompacted) {
			props.setProperty("cleanup.policy", "compact");
		}
		createTestTopic(topic, 3, 1, props);
	}

	private String formatOptions() {
		return "";
	}
}
