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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.source.SearchTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.SearchFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.ml.SearchFunctionProvider;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.FilterCodeGenerator;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.connector.source.SearchRuntimeProviderContext;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class StreamExecVectorSearch extends ExecNodeBase<RowData>
        implements MultipleTransformationTranslator<RowData>, StreamExecNode<RowData> {

    public static final String VECTOR_SEARCH_TRANSFORMATION = "vector-search-table-function";

    FlinkJoinType joinType;
    TemporalTableSourceSpec sourceSpec;

    FunctionCallUtil.FunctionParam topK;
    Map<Integer, FunctionCallUtil.FunctionParam> searchParams;

    public StreamExecVectorSearch(
            ReadableConfig tableConfig,
            FlinkJoinType joinType,
            TemporalTableSourceSpec sourceSpec,
            FunctionCallUtil.FunctionParam topK,
            // key is the indices for the column in the vector table, value is expression to
            // generate code.

            Map<Integer, FunctionCallUtil.FunctionParam> searchParams,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecVectorSearch.class),
                ExecNodeContext.newPersistedConfig(StreamExecVectorSearch.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.joinType = joinType;
        this.sourceSpec = sourceSpec;
        this.topK = topK;
        this.searchParams = searchParams;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        TableSourceTable temporalTable =
                (TableSourceTable)
                        sourceSpec.getTemporalTable(
                                planner.getFlinkContext(), planner.getTypeFactory());
        UserDefinedFunction searchFunction =
                findSearchFunction(
                        createSearchRuntimeProvider(temporalTable, searchParams.keySet()), false);

        Transformation<RowData> inputTransformation =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        FlinkContext context = planner.getFlinkContext();
        DataTypeFactory dataTypeFactory = context.getCatalogManager().getDataTypeFactory();
        RowType inputType = (RowType) getInputEdges().get(0).getOutputType();

        return createSearchFunction(
                inputTransformation,
                config,
                planner.getFlinkContext().getClassLoader(),
                context.getCatalogManager().getDataTypeFactory(),
                inputType,
                (RowType)
                        temporalTable
                                .contextResolvedTable()
                                .getResolvedSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType(),
                (RowType) getOutputType(),
                (SearchFunction) searchFunction);
    }

    private Transformation<RowData> createSearchFunction(
            Transformation<RowData> inputTransformation,
            ExecNodeConfig config,
            ClassLoader classLoader,
            DataTypeFactory dataTypeFactory,
            RowType inputRowType,
            RowType vectorTableOutputType,
            RowType resultRowType,
            SearchFunction searchFunction) {
        ArrayList<FunctionCallUtil.FunctionParam> parameters = new ArrayList<>(2);
        parameters.add(topK);
        parameters.addAll(searchParams.values());
        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher =
                LookupJoinCodeGenerator.generateSyncLookupFunction(
                        config,
                        classLoader,
                        dataTypeFactory,
                        inputRowType,
                        vectorTableOutputType,
                        resultRowType,
                        parameters,
                        searchFunction,
                        "VectorSearch",
                        config.get(PipelineOptions.OBJECT_REUSE));
        GeneratedCollector<ListenableCollector<RowData>> generatedCollector =
                LookupJoinCodeGenerator.generateCollector(
                        new CodeGeneratorContext(config, classLoader),
                        inputRowType,
                        vectorTableOutputType,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        true);
        LookupJoinRunner mlPredictRunner =
                new LookupJoinRunner(
                        generatedFetcher,
                        generatedCollector,
                        FilterCodeGenerator.generateFilterCondition(
                                config, classLoader, null, inputRowType),
                        false,
                        vectorTableOutputType.getFieldCount());
        SimpleOperatorFactory<RowData> operatorFactory =
                SimpleOperatorFactory.of(new ProcessOperator<>(mlPredictRunner));
        return ExecNodeUtil.createOneInputTransformation(
                inputTransformation,
                createTransformationMeta(VECTOR_SEARCH_TRANSFORMATION, config),
                operatorFactory,
                InternalTypeInfo.of(getOutputType()),
                inputTransformation.getParallelism(),
                false);
    }

    public static SearchTableSource.SearchRuntimeProvider createSearchRuntimeProvider(
            TableSourceTable temporalTable, Collection<Integer> searchKeys) {
        // TODO: support nested lookup keys in the future,
        //  currently we only support top-level lookup keys
        int[][] indices = searchKeys.stream().map(i -> new int[] {i}).toArray(int[][]::new);

        SearchTableSource tableSource = (SearchTableSource) temporalTable.tableSource();
        SearchRuntimeProviderContext providerContext = new SearchRuntimeProviderContext(indices);
        return tableSource.getSearchRuntimeProvider(providerContext);
    }

    private UserDefinedFunction findSearchFunction(
            SearchTableSource.SearchRuntimeProvider provider, boolean async) {
        if (async) {
            throw new UnsupportedOperationException("Not implemented yet.");
        } else {
            if (provider instanceof SearchFunctionProvider) {
                return ((SearchFunctionProvider) provider).createSearchFunction();
            }
        }

        throw new TableException(
                "Required "
                        + (async ? "async" : "sync")
                        + " model function by planner, but ModelProvider "
                        + "does not offer a valid model function.");
    }
}
