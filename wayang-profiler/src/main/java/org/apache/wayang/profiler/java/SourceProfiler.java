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

package org.apache.wayang.profiler.java;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} implementation for sinks.
 */
public abstract class SourceProfiler extends OperatorProfiler {

    private JavaChannelInstance outputChannelInstance;

    public SourceProfiler(Supplier<JavaExecutionOperator> operatorGenerator, Supplier<?>... dataQuantumGenerators) {
        super(operatorGenerator, dataQuantumGenerators);
    }

    @Override
    public void prepare(long... inputCardinalities) {
        Validate.isTrue(inputCardinalities.length == 1);

        try {
            this.setUpSourceData(inputCardinalities[0]);
        } catch (Exception e) {
            LogManager.getLogger(this.getClass()).error(
                    String.format("Failed to set up source data for input cardinality %d.", inputCardinalities[0]),
                    e
            );
        }

        super.prepare(inputCardinalities);

        this.outputChannelInstance = createChannelInstance();
    }

    abstract void setUpSourceData(long cardinality) throws Exception;

    @Override
    protected long executeOperator() {
        this.evaluate(
                new JavaChannelInstance[]{},
                new JavaChannelInstance[]{this.outputChannelInstance}
        );
        return this.outputChannelInstance.provideStream().count();
    }

}
