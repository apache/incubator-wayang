package org.qcri.rheem.core.mapping;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.Subplan;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * This factory takes an {@link SubplanMatch} and derives a replacement {@link Subplan} from it.
 */
public abstract class ReplacementSubplanFactory {

    public Operator createReplacementSubplan(SubplanMatch subplanMatch, int epoch) {
        final Operator replacementSubplan = this.translate(subplanMatch, epoch);
        this.checkSanity(subplanMatch, replacementSubplan);
        return replacementSubplan;
    }

    protected void checkSanity(SubplanMatch subplanMatch, Operator replacementSubplan) {
        if (replacementSubplan.getNumInputs() != subplanMatch.getInputMatch().getOperator().getNumInputs()) {
            throw new IllegalStateException("Incorrect number of inputs in the replacement subplan.");
        }
        if (replacementSubplan.getNumOutputs() != subplanMatch.getOutputMatch().getOperator().getNumOutputs()) {
            throw new IllegalStateException("Incorrect number of outputs in the replacement subplan.");
        }
    }

    protected abstract Operator translate(SubplanMatch subplanMatch, int epoch);

    /**
     * Implementation of the {@link ReplacementSubplanFactory}
     * <ul>
     * <li>that replaces exactly one {@link Operator} with exactly one {@link Operator},</li>
     * <li>where both have the exact same {@link InputSlot}s and {@link OutputSlot} in the exact same order.</li>
     * </ul>
     */
    public static class OfSingleOperators<MatchedOperator extends Operator> extends ReplacementSubplanFactory {

        private final BiFunction<MatchedOperator, Integer, Operator> replacementFactory;

        /**
         * Creates a new instance.
         *
         * @param replacementFactory factory for the replacement {@link Operator}s
         */
        public OfSingleOperators(BiFunction<MatchedOperator, Integer, Operator> replacementFactory) {
            this.replacementFactory = replacementFactory;
        }

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            // Extract the single matched Operator.
            final Map<String, OperatorMatch> operatorMatches = subplanMatch.getOperatorMatches();
            Validate.isTrue(operatorMatches.size() == 1);
            final OperatorMatch operatorMatch = operatorMatches.values().stream().findFirst().get();
            final Operator matchedOperator = operatorMatch.getOperator();

            // Create a replacement Operator and align the InputSlots.
            final Operator replacementOperator = this.replacementFactory.apply((MatchedOperator) matchedOperator, epoch);
            for (int inputIndex = matchedOperator.getNumRegularInputs(); inputIndex < matchedOperator.getNumInputs(); inputIndex++) {
                final InputSlot<?> broadcastInput = matchedOperator.getInput(inputIndex);
                Validate.isTrue(broadcastInput.isBroadcast());
                replacementOperator.addBroadcastInput(broadcastInput.copyFor(replacementOperator));
            }

            return replacementOperator;
        }
    }

}
