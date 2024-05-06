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

package org.apache.wayang.ml;

import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.ml.costs.ListwiseCost;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.util.EnumerationStrategy;
import org.apache.wayang.commons.util.profiledb.model.Subject;


/**
 * This is the entry point for users to work with Wayang ML.
 */
public class MLContext extends WayangContext {

    private OrtMLModel model;

    private EnumerationStrategy enumerationStrategy = EnumerationStrategy.NONE;

    public MLContext() {
        super();
    }

    public MLContext(Configuration configuration) {
        super(configuration);
    }

    /**
     * Execute a plan.
     *
     * @param wayangPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    @Override
    public void execute(WayangPlan wayangPlan, String... udfJars) {
        Job wayangJob = this.createJob("", wayangPlan, udfJars);
        OneHotMappings.setOptimizationContext(wayangJob.getOptimizationContext());

        wayangJob.execute();
    }

    public void setModel(OrtMLModel model) {
        this.model = model;
    }

    public void setEnumerationStrategy(EnumerationStrategy strat) {
        this.enumerationStrategy = strat;

        switch(this.enumerationStrategy) {
            case PAIRWISE:
                this.getConfiguration().setCostModel(new PairwiseCost());
                break;
            case LISTWISE:
                this.getConfiguration().setCostModel(new ListwiseCost());
                break;
            default:
                break;
        }
    }
}
