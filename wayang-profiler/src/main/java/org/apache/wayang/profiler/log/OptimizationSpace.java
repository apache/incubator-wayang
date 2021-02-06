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

package org.apache.wayang.profiler.log;

import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Context for the optimization of {@link LoadProfileEstimator}s.
 */
public class OptimizationSpace {
    
    private final Map<String, Variable> variables = new HashMap<>();

    private final List<Variable> variableVector = new ArrayList<>();

    private int numDimensions = 0;
    
    public Variable getOrCreateVariable(String id) {
        final Variable variable = this.variables.computeIfAbsent(id, key -> {
            final Variable newVariable = new Variable(this.numDimensions++, key);
            this.variableVector.add(newVariable);
            return newVariable;
        });

        assert this.variables.size() == this.variableVector.size() :
                String.format("Having %d ID-indexed and %d ordered variables after serving key \"%s\".",
                        this.variables.size(), this.variableVector.size(), id
                );
        return variable;
    }

    public Variable getVariable(String id) {
        return this.variables.get(id);
    }

    public Variable getVariable(int index) {
        return this.variableVector.get(index);
    }

    public Individual createRandomIndividual(Random random) {
        Individual individual = new Individual(this.numDimensions);
        for (Variable variable : variables.values()) {
            variable.setRandomValue(individual, random);
        }
        return individual;
    }

    public List<Variable> getVariables() {
        return this.variableVector;
    }

    public int getNumDimensions() {
        return numDimensions;
    }
}
