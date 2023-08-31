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

import org.apache.flink.table.delegation.PlannerResourceFinderUtil;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.resource.ResourceUri;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlFunction;

import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class PlannerResourceFinderUtilImpl implements PlannerResourceFinderUtil {

    Function<ModifyOperation, RelNode> toRel;
    Function<ResourceUri, URL> resourceLookup;

    public PlannerResourceFinderUtilImpl(
            Function<ModifyOperation, RelNode> toRel, Function<ResourceUri, URL> resourceLookup) {
        this.toRel = toRel;
        this.resourceLookup = resourceLookup;
    }

    @Override
    public Set<URL> findResources(ModifyOperation operation) {
        Set<URL> resources = new HashSet<>();
        toRel.apply(operation).accept(new RelNodeResourceFinder(resources));
        return resources;
    }

    private void add(Set<URL> resources, SqlFunction function) {
        List<ResourceUri> functionResources;
        if (function instanceof BridgingSqlFunction) {
            functionResources =
                    ((BridgingSqlFunction) function).getResolvedFunction().getFunctionResources();
        } else if (function instanceof BridgingSqlAggFunction) {
            functionResources =
                    ((BridgingSqlAggFunction) function)
                            .getResolvedFunction()
                            .getFunctionResources();
        } else {
            functionResources = Collections.emptyList();
        }
        functionResources.forEach(resource -> resources.add(resourceLookup.apply(resource)));
    }

    // --------------------------------------------------------------------------------------------
    // RelNode Utils
    // --------------------------------------------------------------------------------------------

    private class RelNodeResourceFinder extends RelShuttleImpl {

        private final Set<URL> usedResources;
        private final RexNodeResourceFinder finder;

        public RelNodeResourceFinder(Set<URL> usedResources) {
            this.usedResources = usedResources;
            this.finder = new RexNodeResourceFinder(usedResources);
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            aggregate.getAggCallList().forEach(call -> add(usedResources, call.getAggregation()));
            return super.visit(aggregate);
        }

        @Override
        public RelNode visit(TableFunctionScan scan) {
            scan.getCall().accept(finder);
            return super.visit(scan);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            join.getCondition().accept(finder);
            return super.visit(join);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            project.getProjects().forEach(node -> node.accept(finder));
            return super.visit(project);
        }

        @Override
        public RelNode visit(LogicalCalc calc) {
            calc.getProgram().getExprList().forEach(expr -> expr.accept(finder));
            return super.visit(calc);
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            filter.getCondition().accept(finder);
            return super.visit(filter);
        }

        @Override
        public RelNode visit(LogicalMatch match) {
            match.getPatternDefinitions().values().forEach(definition -> definition.accept(finder));
            match.getMeasures().values().forEach(measure -> measure.accept(finder));
            return super.visit(match);
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof TableFunctionScan) {
                // TableFunctionScan doesn't override accept.
                this.visit((TableFunctionScan) other);
            }
            return super.visit(other);
        }
    }

    private class RexNodeResourceFinder extends RexVisitorImpl<Void> {

        private final Set<URL> resources;

        RexNodeResourceFinder(Set<URL> resources) {
            super(true);
            this.resources = resources;
        }

        @Override
        public Void visitCall(RexCall call) {
            if (call.getOperator() instanceof SqlFunction) {
                SqlFunction function = (SqlFunction) (call.getOperator());
                add(resources, function);
            }

            call.getOperands().forEach(operand -> operand.accept(this));
            return null;
        }
    }
}
