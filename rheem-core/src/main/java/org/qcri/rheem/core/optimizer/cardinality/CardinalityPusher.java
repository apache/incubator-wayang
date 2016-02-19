package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

/**
 * Pushes a input {@link CardinalityEstimate}s through an {@link Operator} and yields its output
 * {@link CardinalityEstimate}s. As an important side-effect, {@link Operator}s will store their {@link CardinalityEstimate}
 */
public abstract class CardinalityPusher {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final Operator operator;

    protected CardinalityPusher(Operator operator) {
        this.operator = operator;
    }

    /**
     * Push the input {@link CardinalityEstimate}s of the {@link Operator} of this instance.
     *
     * @param configuration potentially provides some estimation helpers
     * @return the output {@link CardinalityEstimate}s after the push
     */
    public CardinalityEstimate[] push(Configuration configuration) {
        return this.push(configuration, (CardinalityEstimate[]) null);
    }

    /**
     * Push the given {@link CardinalityEstimate}s.
     *
     * @param configuration  potentially provides some estimation helpers
     * @param inputEstimates {@link CardinalityEstimate}s to use
     * @return the output {@link CardinalityEstimate}s after the push
     */
    public CardinalityEstimate[] push(Configuration configuration, CardinalityEstimate... inputEstimates) {
        this.logger.trace("Pushing through {}.", this.operator);
        if (!this.canUpdate()) {
            this.clearAllSlotMarks();
            return this.constructOutputEstimate();
        }

        if (inputEstimates == null) {
            inputEstimates = this.constructInputEstimate();
        }
        this.logger.trace("Pushing {} into {}.", Arrays.toString(inputEstimates), this.getOperator());
        if (!this.canHandle(configuration, inputEstimates)) {
            this.logger.debug("Pushed incomplete estimates to {}... providing fallback estimates.",
                    this.getOperator());
            return this.createFallbackEstimates(configuration, inputEstimates);
        }
        final CardinalityEstimate[] cardinalityEstimates = this.doPush(configuration, inputEstimates);
        this.associateToSlots(cardinalityEstimates);
        this.clearAllSlotMarks();
        return cardinalityEstimates;
    }

    /**
     * @return whether a {@link #doPush(Configuration, CardinalityEstimate...)} execution might result in an update
     * of {@link CardinalityEstimate}s
     */
    protected boolean canUpdate() {
        // We can update if..

        boolean hasUnmarkedOutputEstimates = false;
        for (OutputSlot<?> output : this.operator.getAllOutputs()) {
            // ...there are missing output estimates.
            if (output.getCardinalityEstimate() == null) return true;

            // ...or if there are unmarked output estimates...
            hasUnmarkedOutputEstimates |= !output.isMarked();
        }

        // ...and marked input estimates.
        if (!hasUnmarkedOutputEstimates) return false;
        for (InputSlot<?> input : this.operator.getAllInputs()) {
            if (input.isMarked()) return true;
        }
        return false;
    }

    private void clearAllSlotMarks() {
        for (InputSlot<?> inputSlot : this.operator.getAllInputs()) {
            inputSlot.getAndClearMark();
        }
        for (OutputSlot<?> outputSlot : this.operator.getAllOutputs()) {
            outputSlot.getAndClearMark();
        }
    }

    /**
     * Construct the input {@link CardinalityEstimate} from the {@link Operator}'s {@link InputSlot}s.
     */
    protected CardinalityEstimate[] constructInputEstimate() {
        CardinalityEstimate[] inputEstimate = new CardinalityEstimate[this.operator.getNumInputs()];
        for (int inputIndex = 0; inputIndex < inputEstimate.length; inputIndex++) {
            InputSlot<?> inputSlot = this.operator.getInput(inputIndex);
            inputEstimate[inputIndex] = inputSlot.getCardinalityEstimate();
            assert inputEstimate[inputIndex] != null;
        }
        return inputEstimate;
    }

    /**
     * Construct the output {@link CardinalityEstimate} from the {@link Operator}'s {@link OutputSlot}s.
     */
    protected CardinalityEstimate[] constructOutputEstimate() {
        CardinalityEstimate[] outputEstimate = new CardinalityEstimate[this.operator.getNumOutputs()];
        for (int outputIndex = 0; outputIndex < outputEstimate.length; outputIndex++) {
            OutputSlot<?> output = this.operator.getOutput(outputIndex);
            outputEstimate[outputIndex] = output.getCardinalityEstimate();
            assert outputEstimate[outputIndex] != null;
        }
        return outputEstimate;
    }

    /**
     * Associates the given {@link CardinalityEstimate}s to the {@link OutputSlot}s of the {@link #operator} and their
     * fed {@link InputSlot}s.
     */
    private void associateToSlots(CardinalityEstimate[] cardinalityEstimates) {
        for (int outputIndex = 0; outputIndex < this.getOperator().getNumOutputs(); outputIndex++) {
            final OutputSlot<?> output = this.operator.getOutput(outputIndex);
            final CardinalityEstimate cardinalityEstimate = cardinalityEstimates[outputIndex];
            if (output.isMarked()) {
                this.logger.debug("Did not push {} to marked {}.", cardinalityEstimate, output);
            } else {
                this.operator.propagateOutputCardinality(outputIndex, cardinalityEstimate);
            }
        }
    }

    private boolean canHandle(Configuration configuration, CardinalityEstimate[] inputEstimates) {
        return Arrays.stream(inputEstimates).noneMatch(Objects::isNull);
    }

    private CardinalityEstimate[] createFallbackEstimates(Configuration configuration, CardinalityEstimate[] inputEstimates) {
        return new CardinalityEstimate[this.getOperator().getNumOutputs()];
    }

    protected abstract CardinalityEstimate[] doPush(Configuration configuration, CardinalityEstimate... inputEstimates);

    public Operator getOperator() {
        return this.operator;
    }


}
