/*
 *
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

package org.apache.wayang.ml.test;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.java.platform.JavaPlatform;
import org.junit.BeforeClass;
import org.apache.wayang.ml.costs.MLCost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Superclass for tests of {@link JavaExecutionOperator}s.
 */
public class JavaExecutionTestBase {

    protected static Job job;

    protected static Configuration configuration;

    @BeforeClass
    public static void init() {
        configuration = new Configuration();
        configuration.setCostModel(new MLCost());
        job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
    }

    protected static JavaExecutor createExecutor() {
        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        return new JavaExecutor(JavaPlatform.getInstance(), job);
    }
}
