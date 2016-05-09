package org.qcri.rheem.core.plan.rheemplan.traversal;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Encapsulates logic to traverse a {@link RheemPlan} in a topological, bottom-up manner.
 * <p>In a topological traversal, before a node of a DAG is visited, all it predecessors are visited. Moreover,
 * every node is visited only once. Finally, the nodes can propagate information from predecessor to successor.</p>
 */
public abstract class AbstractTopologicalTraversal<
        ActivatorType extends AbstractTopologicalTraversal.Activator<ActivationType>,
        ActivationType extends AbstractTopologicalTraversal.Activation<ActivatorType>
        > {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Perform a traversal starting from source {@link Operator}s and initially activated {@link Operator}s.
     *
     * @return whether the traversal was <i>not</i> aborted
     */
    public final boolean traverse() {
        try {
            final Queue<ActivatorType> activators = this.initializeActivatorQueue();
            if (activators.isEmpty()) {
                throw new AbortException("No activators available.");
            }
            do {
                final ActivatorType activator = activators.poll();
                // Without this double-cast, we run into a compiler bug: https://bugs.openjdk.java.net/browse/JDK-8131744
                activator.process((Queue<Activator<ActivationType>>) (Queue) activators);
            } while (!activators.isEmpty());
        } catch (AbortException e) {
            this.logger.debug("Traversal aborted: {}", e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Set up a queue of initial {@link Activator}s for an estimation pass.
     */
    private final Queue<ActivatorType> initializeActivatorQueue() {
        // Set up the initial Activators.
        Queue<ActivatorType> activatorQueue = new LinkedList<>(this.getInitialActivators());

        // Fire Activations satisfied from the payloads.
        for (int i = 0; i < this.getNumInitialActivations(); i++) {
            final Collection<ActivationType> activations = this.getInitialActivations(i);
            for (ActivationType activation : activations) {
                ActivatorType activator = activation.getTargetActivator();
                activator.accept(activation);
                if (activator.isActivationComplete()) {
                    activatorQueue.add(activator);
                }
            }
        }
        return activatorQueue;
    }

    protected abstract Collection<ActivatorType> getInitialActivators();

    protected abstract Collection<ActivationType> getInitialActivations(int index);

    protected abstract int getNumInitialActivations();

    /**
     * Wraps a {@link CardinalityEstimator}, thereby caching its input {@link CardinalityEstimate}s and keeping track
     * of its dependent {@link CardinalityEstimator}s.
     */
    public static abstract class Activator<TActivation extends Activation<? extends Activator<TActivation>>> {

        protected final Operator operator;

        protected Activator(Operator operator) {
            this.operator = operator;
        }

        protected abstract boolean isActivationComplete();

        /**
         * Execute this instance, thereby activating new instances and putting them on the queue.
         *
         * @param activatorQueue accepts newly activated {@link CardinalityEstimator}s
         */
        protected void process(Queue<Activator<TActivation>> activatorQueue) {
            assert this.isActivationComplete() : String.format("Cannot process %s: activation not complete.", this);
            Collection<TActivation> successorActivations = this.doWork();
            if (successorActivations == null) {
                throw new AbortException(String.format("%s requested to abort.", this));
            }

            for (TActivation activation : successorActivations) {
                final Activator<TActivation> activator = activation.getTargetActivator();
                activator.accept(activation);
                if (activator.isActivationComplete()) {
                    activatorQueue.add(activator);
                }
            }
        }

        /**
         * Performs the work to be done by this instance and defines the next {@link Activation}s.
         *
         * @return the newly produced {@link Activation}s or {@code null} if traversal should be aborted
         */
        protected abstract Collection<TActivation> doWork();

        protected abstract void accept(TActivation activation);

        @Override
        public String toString() {
            return String.format("%s[%s]", this.getClass().getSimpleName(), this.operator);
        }
    }

    /**
     * Describes a reference to an input of an {@link Activator}.
     */
    public abstract static class Activation<TActivator extends Activator<? extends Activation<TActivator>>> {

        private final TActivator targetActivator;

        protected Activation(TActivator targetActivator) {
            this.targetActivator = targetActivator;
        }

        protected TActivator getTargetActivator() {
            return this.targetActivator;
        }

    }

    /**
     * Declares that the current traversal should be aborted.
     */
    public static class AbortException extends RheemException {

        public AbortException(String message) {
            super(message);
        }

    }

}
