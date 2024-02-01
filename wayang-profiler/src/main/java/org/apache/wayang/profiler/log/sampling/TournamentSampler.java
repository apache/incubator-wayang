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
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Sampling strategy that simulates a tournament between elements.
 */
public class TournamentSampler<T> implements Sampler<T> {

    final Random random = new Random();

    @Override
    public List<T> sample(Collection<T> set, Battle<T> battle, double selectionProbability) {
        ArrayList<T> arena = new ArrayList<>(set.size());
        arena.addAll(set);
        Collections.shuffle(arena, this.random);

        int targetSize = (int) Math.max(1, Math.round(set.size() * selectionProbability));
        while (arena.size() >= 2 * targetSize) {
            // Do tournaments.
            for (int i = 0; i < arena.size() / 2; i++) {
                T t1 = arena.get(2 * i);
                T t2 = arena.get(2 * i + 1);
                arena.set(i, battle.battle(t1, t2));
            }
            // Make sure not to forget an odd-indexed, pending element.
            if (arena.size() % 2 == 1) {
                arena.set(arena.size() / 2, arena.get(arena.size() - 1));
            }
            // Trim the array.
            int newSize = (arena.size() + 1) / 2;
            for (int i = arena.size() - 1; i >= newSize; i--) {
                arena.remove(i);
            }
        }

        // Do the remaining battles.
        int pendingElements = arena.size() - targetSize;
        // Do tournaments.
        for (int i = 0; i < pendingElements; i++) {
            T t1 = arena.get(2 * i);
            T t2 = arena.get(2 * i + 1);
            arena.set(i, battle.battle(t1, t2));
        }
        // Shift the remaining elements without battling.
        for (int i = arena.size() - 1; i >= targetSize; i--) {
            T element = arena.remove(i);
            arena.set(i - pendingElements, element);
        }
        return arena;
    }

}
