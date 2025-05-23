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

package org.apache.wayang.api.sql.calcite.converter.functions;

import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

public class FilterPredicateImpl implements FunctionDescriptor.SerializablePredicate<Record> {
    private final Node<Object> callTree;

    public FilterPredicateImpl(final RexNode condition) {
        this.callTree = new FilterCallTreeFactory().fromRexNode(condition);
    }

    @Override
    public boolean test(final Record record) {
        return (boolean) callTree.evaluate(record);
    }

    class FilterCallTreeFactory implements CallTreeFactory<List<Object>, Object> {
        public SerializableFunction<List<Object>, Object> deriveOperation(final SqlKind kind) {
            return (input) -> switch (kind) {
                case NOT -> !(boolean) input.get(0);
                case IS_NOT_NULL -> !isEqualTo(input.get(0), null);
                case IS_NULL -> isEqualTo(input.get(0), null);
                case LIKE -> like((String) input.get(0), (String) input.get(1));
                case NOT_EQUALS -> !isEqualTo(input.get(0), input.get(1));
                case EQUALS -> isEqualTo(input.get(0), input.get(1));
                case GREATER_THAN -> isGreaterThan(input.get(0), input.get(1));
                case LESS_THAN -> isLessThan(input.get(0), input.get(1));
                case GREATER_THAN_OR_EQUAL ->
                    isGreaterThan(input.get(0), input.get(1)) || isEqualTo(input.get(0), input.get(1));
                case LESS_THAN_OR_EQUAL ->
                    isLessThan(input.get(0), input.get(1)) || isEqualTo(input.get(0), input.get(1));
                case AND -> input.stream().map(Boolean.class::cast).allMatch(Boolean::booleanValue);
                case OR -> input.stream().map(Boolean.class::cast).anyMatch(Boolean::booleanValue);
                default -> throw new UnsupportedOperationException("Kind not supported: " + kind);
            };
        }
    }

    /**
     * Widening conversions
     */
    final SerializableFunction<Object, Comparable> ensureComparable = (a) -> a instanceof Integer val ? val.longValue() : (Comparable<?>) a;

    /**
     * Java equivalent of SQL like clauses
     * @param s1
     * @param s2
     * @return true if {@code s1} like {@code s2}
     */
    private boolean like(final String s1, final String s2) {
        final SqlFunctions.LikeFunction likeFunction = new SqlFunctions.LikeFunction();
        final boolean isMatch = likeFunction.like(s1, s2);

        return isMatch;
    }

    /**
     * Java equivalent of sql greater than clauses
     * @param o1
     * @param o2
     * @return true if {@code o1 > o2}
     */
    private boolean isGreaterThan(final Object o1, final Object o2) {
        return ensureComparable.apply(o1).compareTo(ensureComparable.apply(o2)) > 0;
    }

    /**
     * Java equivalent of sql less than clauses
     * @param o1
     * @param o2
     * @return true if {@code o1 < o2}
     */
    private boolean isLessThan(final Object o1, final Object o2) {
        return ensureComparable.apply(o1).compareTo(ensureComparable.apply(o2)) < 0;
    }

    /**
     * Java equivalent of SQL equals clauses
     * @param o1
     * @param o2
     * @return true if {@code o1 == o2}
     */
    private boolean isEqualTo(final Object o1, final Object o2) {
        return ensureComparable.apply(o1).equals(ensureComparable.apply(o2));
    }
}