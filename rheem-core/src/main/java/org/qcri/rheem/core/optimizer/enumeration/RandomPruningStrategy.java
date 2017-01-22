package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

/**
 * This {@link PlanEnumerationPruningStrategy} retains only the best {@code k} {@link PlanImplementation}s.
 */
@SuppressWarnings("unused")
public class RandomPruningStrategy implements PlanEnumerationPruningStrategy {

    private Random random;

    private int numRetainedPlans;

    @Override
    public void configure(Configuration configuration) {
        this.numRetainedPlans = (int) configuration.getLongProperty("rheem.core.optimizer.pruning.random.retain", 1);
        long seed = configuration.getLongProperty("rheem.core.optimizer.pruning.random.seed", System.currentTimeMillis());
        this.random = new Random(seed);
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        if (planEnumeration.getPlanImplementations().size() <= this.numRetainedPlans) return;

        ArrayList<PlanImplementation> planImplementations = new ArrayList<>(planEnumeration.getPlanImplementations());
        Collections.shuffle(planImplementations, this.random);
        planEnumeration.getPlanImplementations().retainAll(planImplementations.subList(0, this.numRetainedPlans));
    }
}
