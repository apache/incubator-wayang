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

package org.apache.wayang.spark.mapping.graph;

import org.apache.wayang.basic.operators.PageRankOperator;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link PageRankOperator} to org.apache.wayang.spark.operators.graph.SparkPageRankOperator .
 */
public class PageRankMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "pageRank", new PageRankOperator(1), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<PageRankOperator>(
                (matchedOperator, epoch) -> {
                    // We need to instantiate the SparkPageRankOperator via reflection, because the Scala code will
                    // be compiled only after the Java code, which might cause compile errors.
                    try {
                        final Class<?> cls = Class.forName("org.apache.wayang.spark.operators.graph.SparkPageRankOperator");
                        final Constructor<?> constructor = cls.getConstructor(PageRankOperator.class);
                        return (Operator) constructor.newInstance(matchedOperator);
                    } catch (Exception e) {
                        throw new WayangException(String.format("Could not apply %s.", this), e);
                    }
                }
        );
    }
}
