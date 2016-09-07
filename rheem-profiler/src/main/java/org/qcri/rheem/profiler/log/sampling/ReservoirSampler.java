package org.qcri.rheem.profiler.log.sampling;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Utility to do reservoir sampling with competitions over shared space.
 */
public abstract class ReservoirSampler<T> implements Sampler<T> {

    private final ArrayList<T> reservoir;

    private final Random random = new Random();

    public ReservoirSampler(int sampleSize) {
        this.reservoir = new ArrayList<>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            this.reservoir.add(null);
        }
    }

    @Override
    public List<T> sample(Collection<T> set, Battle<T> battle, double selectionProbability) {
        // Clear the reservoir.
        for (int i = 0; i < this.reservoir.size(); i++) {
            this.reservoir.set(i, null);
        }

        for (T candidate : set) {
            // Designate a position for the candidate.
            int index = this.random.nextInt(this.reservoir.size());
            T existing = this.reservoir.get(index);

            if (existing == null) {
                this.reservoir.set(index, candidate);
            } else {
                // Let the candidates compete for the slot in the reservoir.
                final T winner = battle.battle(existing, candidate);
                this.reservoir.set(index, winner);
            }
        }
//        return set;
        throw new UnsupportedOperationException("Not properly implemented.");
    }

    abstract protected T letCompete(T candidate1, T candidate2, Random random);

}
