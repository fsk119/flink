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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.gateway.ClientResult;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.context.ExecutionContext;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.header.operation.CloseOperationHeaders;
import org.apache.flink.table.gateway.rest.header.session.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.GetSessionConfigHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.statement.ExecuteStatementHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.operation.OperationMessageParameters;
import org.apache.flink.table.gateway.rest.message.session.CloseSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.GetSessionConfigResponseBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementRequestBody;
import org.apache.flink.table.gateway.rest.message.statement.ExecuteStatementResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsTokenParameters;
import org.apache.flink.table.gateway.rest.serde.ResultInfo;
import org.apache.flink.table.gateway.rest.serde.RowDataInfo;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.rest.handler.session.CloseSessionHandler.CLOSE_MESSAGE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Executor that performs the Flink communication locally. The calls are blocking depending on the
 * response time to the Flink cluster. Flink jobs are not blocking.
 */
public class LocalExecutor implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutor.class);

    // result maintenance

    private final ExecutorService executorService;
    private final Configuration configuration;
    private final InetSocketAddress socketAddress;

    private RestClient restClient;

    private SessionHandle sessionHandle;
    private SessionMessageParameters sessionMessageParametersInstance;

    /** Creates a local executor for submitting table programs and retrieving results. */
    public LocalExecutor(InetSocketAddress socketAddress, Configuration configuration) {
        this.socketAddress = socketAddress;
        this.configuration = configuration;
        this.executorService = Executors.newFixedThreadPool(2);
    }

    @Override
    public void open(@Nullable String sessionId) throws SqlExecutionException {
        try {
            this.restClient = new RestClient(configuration, executorService);
        } catch (ConfigurationException e) {
            throw new SqlExecutionException("Failed to create the client.", e);
        }

        LOG.info("Open session  to {}:{}.", socketAddress.getAddress(), socketAddress.getPort());
        // Open session to address:port and get the session handle ID
        OpenSessionRequestBody request =
                new OpenSessionRequestBody(sessionId, configuration.toMap());
        try {
            OpenSessionResponseBody response =
                    sendRequest(
                                    OpenSessionHeaders.getInstance(),
                                    EmptyMessageParameters.getInstance(),
                                    request)
                            .get();
            sessionHandle = new SessionHandle(UUID.fromString(response.getSessionHandle()));
        } catch (Exception e) {
            LOG.error(
                    String.format(
                            "Failed to open session to %s:%s",
                            socketAddress.getAddress(), socketAddress.getPort()),
                    e);
            throw new SqlClientException(
                    String.format(
                            "Failed to open session to %s:%s",
                            socketAddress.getAddress(), socketAddress.getPort()),
                    e);
        }
        sessionMessageParametersInstance = new SessionMessageParameters(sessionHandle);
    }

    @Override
    public void close() throws SqlExecutionException {
        executorService.shutdownNow();
        // close session
        try {
            CompletableFuture<CloseSessionResponseBody> response =
                    sendRequest(
                            CloseSessionHeaders.getInstance(),
                            sessionMessageParametersInstance,
                            EmptyRequestBody.getInstance());

            if (!response.get().getStatus().equals(CLOSE_MESSAGE)) {
                LOG.warn("The status of close session response isn't {}.", CLOSE_MESSAGE);
            }
        } catch (Throwable t) {
            LOG.warn(
                    String.format(
                            "Unexpected error occurs when closing session %s.", sessionHandle),
                    t);
            // ignore any throwable to keep the cleanup running
        }
    }

    /**
     * Get the existed {@link ExecutionContext} from contextMap, or thrown exception if does not
     * exist.
     */
    public Map<String, String> getSessionConfigMap() throws SqlExecutionException {
        try {
            CompletableFuture<GetSessionConfigResponseBody> response =
                    sendRequest(
                            GetSessionConfigHeaders.getInstance(),
                            sessionMessageParametersInstance,
                            EmptyRequestBody.getInstance());
            return response.get().getProperties();
        } catch (Exception e) {
            LOG.error("Failed to get session config.", e);
            throw new SqlExecutionException("Failed to get session config.", e);
        }
    }

    @Override
    public ReadableConfig getSessionConfig() throws SqlExecutionException {
        return Configuration.fromMap(getSessionConfigMap());
    }

    @Override
    public void resetSessionProperties() throws SqlExecutionException {}

    @Override
    public void resetSessionProperty(String key) throws SqlExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSessionProperty(String key, String value) throws SqlExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClientResult executeStatement(String statement)
            throws SqlExecutionException, SqlParserEOFException {
        ExecuteStatementRequestBody request = new ExecuteStatementRequestBody(statement, 0L, null);
        try {
            CompletableFuture<ExecuteStatementResponseBody> executeStatementResponse =
                    sendRequest(
                            ExecuteStatementHeaders.getInstance(),
                            sessionMessageParametersInstance,
                            request);

            OperationHandle operationHandle =
                    new OperationHandle(
                            UUID.fromString(executeStatementResponse.get().getOperationHandle()));

            // TODO: introduce option later
            FetchResultsResponseBody fetchResultsResponse = fetchWhenResultsReady(operationHandle);
            ResultInfo firstResult = fetchResultsResponse.getResults();

            return new ClientResult(
                    checkNotNull(fetchResultsResponse.isQuery()),
                    ResolvedSchema.of(
                            fetchResultsResponse.getResults().getColumnInfo().stream()
                                    .map(
                                            col ->
                                                    Column.physical(
                                                                    col.getName(),
                                                                    DataTypeUtils
                                                                            .toInternalDataType(
                                                                                    col
                                                                                            .getLogicalType()))
                                                            .withComment(col.getComment()))
                                    .collect(Collectors.toList())),
                    fetchResultsResponse.getJobID().orElse(null),
                    new RowDataInfoIterator(
                            operationHandle,
                            firstResult.getRowDataInfo(),
                            parseTokenFromUri(fetchResultsResponse.getNextResultUri())));

        } catch (Exception e) {
            LOG.error("Unexpected error occurs when executing SQL statement.", e);
            throw new SqlExecutionException(
                    "Unexpected error occurs when executing SQL statement.", e);
        }
    }

    @Override
    public List<String> completeStatement(String statement, int position) {
        //        final ExecutionContext context = getExecutionContext();
        //        final TableEnvironmentInternal tableEnv =
        //                (TableEnvironmentInternal) context.getTableEnvironment();
        //
        //        try {
        //            return Arrays.asList(tableEnv.getParser().getCompletionHints(statement,
        // position));
        //        } catch (Throwable t) {
        //            // catch everything such that the query does not crash the executor
        //            if (LOG.isDebugEnabled()) {
        //                LOG.debug("Could not complete statement at " + position + ":" + statement,
        // t);
        //            }
        //            return Collections.emptyList();
        //        }
        throw new UnsupportedOperationException("Not implemented.");
    }

    // ---------------------------------------------------------------------------------------------

    private class RowDataInfoIterator implements CloseableIterator<RowDataInfo> {

        private final OperationHandle operationHandle;
        private Iterator<RowDataInfo> currentBuffer;
        private Long nextToken;

        public RowDataInfoIterator(
                OperationHandle operationHandle, List<RowDataInfo> buffer, Long nextToken) {
            this.operationHandle = operationHandle;
            this.currentBuffer = buffer.iterator();
            this.nextToken = nextToken;
        }

        @Override
        public void close() throws Exception {
            sendRequest(
                    CloseOperationHeaders.getInstance(),
                    new OperationMessageParameters(sessionHandle, operationHandle),
                    EmptyRequestBody.getInstance());
        }

        @Override
        public boolean hasNext() {
            if (!currentBuffer.hasNext()) {
                while (nextToken != null && !currentBuffer.hasNext()) {
                    FetchResultsResponseBody fetchResultsResponseBody =
                            fetchResults(operationHandle, nextToken);
                    nextToken = parseTokenFromUri(fetchResultsResponseBody.getNextResultUri());
                    currentBuffer =
                            fetchResultsResponseBody.getResults().getRowDataInfo().iterator();
                }
            }
            return currentBuffer.hasNext();
        }

        @Override
        public RowDataInfo next() {
            return currentBuffer.next();
        }
    }

    private <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request)
                    throws IOException {
        return restClient.sendRequest(
                socketAddress.getHostName(),
                socketAddress.getPort(),
                messageHeaders,
                messageParameters,
                request);
    }

    @SuppressWarnings("BusyWait")
    private FetchResultsResponseBody fetchWhenResultsReady(OperationHandle operationHandle)
            throws SqlClientException {
        Function<FetchResultsResponseBody, Boolean> wait =
                response -> response.getResultType().equals(ResultSet.ResultType.NOT_READY);
        FetchResultsResponseBody response = fetchResults(operationHandle);

        while (wait.apply(response)) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                throw new SqlClientException(e);
            }
            response = fetchResults(operationHandle);
        }

        if (wait.apply(response)) {
            LOG.error(
                    "Failed to fetch results within timeout. OperationHandle ID: {}.",
                    operationHandle);
            throw new SqlClientException(
                    String.format(
                            "Failed to fetch results within timeout. OperationHandle ID: %s.",
                            operationHandle));
        }

        return response;
    }

    private FetchResultsResponseBody fetchResults(OperationHandle operationHandle) {
        return fetchResults(operationHandle, 0L);
    }

    public FetchResultsResponseBody fetchResults(OperationHandle operationHandle, long token)
            throws SqlClientException {
        FetchResultsTokenParameters fetchResultsTokenParameters =
                new FetchResultsTokenParameters(sessionHandle, operationHandle, token);
        try {
            return sendRequest(
                            FetchResultsHeaders.getInstance(),
                            fetchResultsTokenParameters,
                            EmptyRequestBody.getInstance())
                    .get();
        } catch (Exception e) {
            LOG.error(
                    String.format(
                            "Unexpected error occurs when fetching results. OperationHandle ID: %s.",
                            operationHandle),
                    e);
            throw new SqlExecutionException(
                    String.format(
                            "Unexpected error occurs when fetching results. OperationHandle ID: %s.",
                            operationHandle),
                    e);
        }
    }

    public Long parseTokenFromUri(String uri) {
        if (uri == null || uri.length() == 0) {
            return null;
        }
        String[] split = uri.split("/");
        return Long.valueOf(split[split.length - 1]);
    }
}
