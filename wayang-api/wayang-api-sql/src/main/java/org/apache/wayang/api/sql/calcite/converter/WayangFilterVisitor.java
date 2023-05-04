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

package org.apache.wayang.api.sql.calcite.converter;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.wayang.api.sql.calcite.rel.WayangFilter;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class WayangFilterVisitor extends WayangRelNodeVisitor<WayangFilter> {
    WayangFilterVisitor(WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(WayangFilter wayangRelNode) {

        Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput(0));

        RexNode condition = ((Filter) wayangRelNode).getCondition();

        FilterOperator<Record> filter = new FilterOperator(
                new FilterPredicateImpl(condition),
                Record.class
        );

        childOp.connectTo(0,filter,0);

        return filter;
    }


    private class FilterPredicateImpl implements FunctionDescriptor.SerializablePredicate<Record> {

        private final RexNode condition;

        private FilterPredicateImpl(RexNode condition) {
            this.condition = condition;
        }

        @Override
        public boolean test(Record record) {
            return condition.accept(new EvaluateFilterCondition(true, record));
        }
    }


    private class EvaluateFilterCondition extends RexVisitorImpl<Boolean> {

        final Record record;
        protected EvaluateFilterCondition(boolean deep, Record record) {
            super(deep);
            this.record = record;
        }

        @Override
        public Boolean visitCall(RexCall call) {
            SqlKind kind = call.getKind();
            if(!kind.belongsTo(SUPPORTED_OPS)) {
                throw new IllegalStateException("Cannot handle this filter predicate yet");
            }

            RexNode leftOperand = call.getOperands().get(0);
            RexNode rightOperand = call.getOperands().get(1);

            if(kind == SqlKind.AND) {
                return leftOperand.accept(this) && rightOperand.accept(this);
            } else if(kind == SqlKind.OR) {
                return leftOperand.accept(this) || rightOperand.accept(this);
            } else {
                return eval(record, kind, leftOperand, rightOperand);
            }
        }

        public boolean eval(Record record, SqlKind kind, RexNode leftOperand, RexNode rightOperand) {

            if(leftOperand instanceof RexInputRef && rightOperand instanceof RexLiteral) {
                RexInputRef rexInputRef = (RexInputRef)leftOperand;
                int index = rexInputRef.getIndex();
                Object field = record.getField(index);
                RexLiteral rexLiteral = (RexLiteral) rightOperand;
                switch (kind) {
                    case GREATER_THAN:
                        return isGreaterThan(field, rexLiteral);
                    case LESS_THAN:
                        return isLessThan(field, rexLiteral);
                    case EQUALS:
                        return isEqualTo(field, rexLiteral);
                    case GREATER_THAN_OR_EQUAL:
                        return isGreaterThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                    case LESS_THAN_OR_EQUAL:
                        return isLessThan(field, rexLiteral) || isEqualTo(field, rexLiteral);
                    default:
                        throw new IllegalStateException("Predicate not supported yet");

                }

            } else {
                throw new IllegalStateException("Predicate not supported yet");
            }

        }

        private boolean isGreaterThan(Object o, RexLiteral rexLiteral) {
//            return rexLiteral.getValue().compareTo(o)< 0;
            return ((Comparable)o).compareTo(rexLiteral.getValueAs(o.getClass())) > 0;

        }

        private boolean isLessThan(Object o, RexLiteral rexLiteral) {
            return ((Comparable)o).compareTo(rexLiteral.getValueAs(o.getClass())) < 0;
        }

        private boolean isEqualTo(Object o, RexLiteral rexLiteral) {
            try {
                return ((Comparable)o).compareTo(rexLiteral.getValueAs(o.getClass())) == 0;
            } catch (Exception e) {
                throw new IllegalStateException("Predicate not supported yet");
            }
        }
    }

    /**for quick sanity check **/
    private static final EnumSet<SqlKind> SUPPORTED_OPS =
            EnumSet.of(SqlKind.AND, SqlKind.OR,
                    SqlKind.EQUALS, SqlKind.NOT_EQUALS,
                    SqlKind.LESS_THAN, SqlKind.GREATER_THAN,
                    SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL);








}
