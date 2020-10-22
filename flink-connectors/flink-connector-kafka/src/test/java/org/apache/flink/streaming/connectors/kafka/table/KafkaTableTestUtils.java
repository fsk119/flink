package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utils used by IT cases.
 */
public class KafkaTableTestUtils {

	public static final class TestingSinkFunction implements SinkFunction<RowData> {

		private static final long serialVersionUID = 455430015321124493L;
		private static List<String> rows = new ArrayList<>();

		private final int expectedSize;

		private TestingSinkFunction(int expectedSize) {
			this.expectedSize = expectedSize;
			rows.clear();
		}

		@Override
		public void invoke(RowData value, Context context) {
			rows.add(value.toString());
			if (rows.size() >= expectedSize) {
				// job finish
				throw new SuccessException();
			}
		}
	}

	public static boolean isCausedByJobFinished(Throwable e) {
		if (e instanceof SuccessException) {
			return true;
		} else if (e.getCause() != null) {
			return isCausedByJobFinished(e.getCause());
		} else {
			return false;
		}
	}

	public static List<Row> collectRows(Table table, int expectedSize) throws Exception {
		final TableResult result = table.execute();
		final List<Row> collectedRows = new ArrayList<>();
		try (CloseableIterator<Row> iterator = result.collect()) {
			while (collectedRows.size() < expectedSize && iterator.hasNext()) {
				collectedRows.add(iterator.next());
			}
		}
		result.getJobClient().ifPresent(jc -> {
			try {
				jc.cancel().get(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		return collectedRows;
	}
}
