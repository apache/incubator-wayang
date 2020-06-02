package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;

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
