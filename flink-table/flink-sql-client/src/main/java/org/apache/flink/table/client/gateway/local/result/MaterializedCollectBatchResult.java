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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;

import java.util.List;

/** Collects results and returns them as table snapshots. */
public class MaterializedCollectBatchResult extends MaterializedCollectResultBase {

    @VisibleForTesting
    public MaterializedCollectBatchResult(
            TableResult tableResult, int maxRowCount, int overcommitThreshold) {
        super(tableResult, maxRowCount, overcommitThreshold);
        // start listener thread
        retrievalThread.start();
    }

    public MaterializedCollectBatchResult(TableResult tableResult, int maxRowCount) {
        this(tableResult, maxRowCount, computeMaterializedTableOvercommit(maxRowCount));
    }

    @VisibleForTesting
    protected List<Row> getMaterializedTable() {
        return materializedTable;
    }

    @Override
    protected void processRecord(Row row) {
        // limit the materialized table
        if (materializedTable.size() - validRowPosition >= maxRowCount) {
            cleanUp();
        }
        materializedTable.add(row);
    }

    @Override
    public TypedResult<Integer> snapshot(int pageSize) {
        synchronized (resultLock) {
            // the job finished with an exception
            SqlExecutionException e = executionException.get();
            if (e != null) {
                throw e;
            }

            // wait for finish
            if (isRetrieving()) {
                return TypedResult.empty();
            }
            // we return a payload result the first time and EoS for the rest of times as if the
            // results
            // are retrieved dynamically
            else if (!isLastSnapshot) {
                isLastSnapshot = true;
                this.pageSize = pageSize;
                snapshot.clear();
                for (int i = validRowPosition; i < materializedTable.size(); i++) {
                    snapshot.add(materializedTable.get(i));
                }
                pageCount = Math.max(1, (int) Math.ceil(((double) snapshot.size() / pageSize)));
                return TypedResult.payload(pageCount);
            } else {
                return TypedResult.endOfStream();
            }
        }
    }

    private void cleanUp() {
        materializedTable.set(validRowPosition, null);
        validRowPosition++;

        // perform clean up in batches
        if (validRowPosition >= overcommitThreshold) {
            materializedTable.subList(0, validRowPosition).clear();
            validRowPosition = 0;
        }
    }
}
