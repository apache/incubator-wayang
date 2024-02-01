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

package org.apache.wayang.profiler.log.sampling;

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
