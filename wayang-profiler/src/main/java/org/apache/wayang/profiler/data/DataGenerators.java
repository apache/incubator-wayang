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

package org.apache.wayang.profiler.data;

import org.apache.commons.lang3.Validate;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

/**
 * Utility to create common data generators.
 */
public class DataGenerators {

    private static final String[] CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".split("");

    public static Generator<String> createReservoirBasedStringSupplier(List<String> stringReservoir,
                                                                      double reuseProbability,
                                                                      Random random,
                                                                      int minLen,
                                                                      int maxLen) {
        return () -> {
            if (random.nextDouble() > reuseProbability || stringReservoir.isEmpty()) {
                final String randomString = createRandomString(minLen, maxLen, random);
                stringReservoir.add(randomString);
                return randomString;
            } else {
                return stringReservoir.get(random.nextInt(stringReservoir.size()));
            }
        };
    }

    public static Generator<String> createRandomStringSupplier(int minLen, int maxLen, Random random) {
        return () -> createRandomString(minLen, maxLen, random);
    }

    private static String createRandomString(int minLen, int maxLen, Random random) {
        int len = (minLen == maxLen) ? minLen : (random.nextInt(maxLen - minLen) + minLen);
        StringBuilder sb = new StringBuilder(len);
        while (sb.length() < len) {
            sb.append(CHARACTERS[random.nextInt(CHARACTERS.length)]);
        }
        return sb.toString();
    }

    public static Generator<Integer> createReservoirBasedIntegerSupplier(List<Integer> reservoir,
                                                                        double reuseProbability,
                                                                        Random random) {
        return () -> {
            if (random.nextDouble() > reuseProbability || reservoir.isEmpty()) {
                final Integer randomInteger = random.nextInt();
                reservoir.add(randomInteger);
                return randomInteger;
            } else {
                return reservoir.get(random.nextInt(reservoir.size()));
            }
        };
    }

    public static Generator<Integer> createRandomIntegerSupplier(Random random) {
        return random::nextInt;
    }

    public static Generator<Integer> createRandomIntegerSupplier(int min, int max, Random random) {
        Validate.isTrue(min <= max);
        return () -> min + random.nextInt(max - min);
    }

    public interface Generator<T> extends Supplier<T>, Serializable {
    }

}
