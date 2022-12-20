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

package org.apache.flink.table.client.gateway;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.rest.serde.RowDataInfo;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class ClientResult implements Closeable, Iterator<RowDataInfo> {

    private final boolean isQuery;
    private final ResolvedSchema resultSchema;
    private @Nullable final JobID jobID;
    private final CloseableIterator<RowDataInfo> rows;

    public ClientResult(
            boolean isQuery,
            ResolvedSchema resultSchema,
            @Nullable JobID jobID,
            CloseableIterator<RowDataInfo> rows) {
        this.isQuery = isQuery;
        this.resultSchema = resultSchema;
        this.jobID = jobID;
        this.rows = rows;
    }

    public boolean isQueryResult() {
        return isQuery;
    }

    public boolean hasNext() {
        return rows.hasNext();
    }

    public ResolvedSchema getResultSchema() {
        return resultSchema;
    }

    public @Nullable JobID getJobID() {
        return jobID;
    }

    @Override
    public RowDataInfo next() {
        return rows.next();
    }

    @Override
    public void close() throws IOException {}

    public Iterator<RowData> toRowDataIterator() {
        return new Iterator<RowData>() {

            @Override
            public boolean hasNext() {
                return ClientResult.this.hasNext();
            }

            @Override
            public RowData next() {
                return ClientResult.this.next().toRowData();
            }
        };
    }
}
