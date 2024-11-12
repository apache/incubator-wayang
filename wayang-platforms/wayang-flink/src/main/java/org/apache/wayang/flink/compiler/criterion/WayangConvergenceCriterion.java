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

package org.apache.wayang.flink.compiler.criterion;


import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.wayang.core.function.FunctionDescriptor;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Class create a {@Link ConvergenceCriterion} that generate aggregatorWrapper
 */
public class WayangConvergenceCriterion<T>
        implements ConvergenceCriterion<WayangListValue>, Serializable {

    private boolean doWhile;
    private FunctionDescriptor.SerializablePredicate<Collection<T>> predicate;

    public WayangConvergenceCriterion(FunctionDescriptor.SerializablePredicate<Collection<T>> predicate){
        this.predicate = predicate;
        this.doWhile = false;
    }

    public WayangConvergenceCriterion setDoWhile(boolean doWhile){
        this.doWhile = doWhile;
        return this;
    }

    @Override
    public boolean isConverged(int i, WayangListValue tWayangValue) {
        if( i == 1 && this.doWhile ){
            return true;
        }
        Collection collection = tWayangValue
                .stream()
                .map(
                    wayangValue -> wayangValue.get()
                ).collect(
                    Collectors.toList()
        );
        return this.predicate.test(collection);
    }

}
