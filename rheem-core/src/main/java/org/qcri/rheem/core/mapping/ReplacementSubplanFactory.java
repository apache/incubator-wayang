package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.Subplan;

/**
 * This factory takes an {@link SubplanMatch} and derives a replacement {@link Subplan} from it.
 */
public abstract class ReplacementSubplanFactory {

    public Subplan createReplacementSubplan(SubplanMatch subplanMatch) {
        final Subplan replacementSubplan = translate(subplanMatch);
        checkSanity(subplanMatch, replacementSubplan);
        return replacementSubplan;
    }

    protected void checkSanity(SubplanMatch subplanMatch, Subplan replacementSubplan) {
        if (replacementSubplan.getNumInputs() != subplanMatch.getPattern().getNumInputs()) {
            throw new IllegalStateException("Incorrect number of inputs in the replacement subplan.");
        }
        if (replacementSubplan.getNumOutputs() != subplanMatch.getPattern().getNumOutputs()) {
            throw new IllegalStateException("Incorrect number of outputs in the replacement subplan.");
        }
    }

    protected abstract Subplan translate(SubplanMatch subplanMatch);

}
