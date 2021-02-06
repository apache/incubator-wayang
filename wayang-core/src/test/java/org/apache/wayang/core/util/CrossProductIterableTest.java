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

package org.apache.wayang.core.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Test suite for {@link CrossProductIterable}.
 */
public class CrossProductIterableTest {

    @Test
    public void test2x3() {
        List<List<Integer>> matrix = Arrays.asList(
                Arrays.asList(1, 2 ,3),
                Arrays.asList(4, 5, 6)
        );
        final Iterable<List<Integer>> crossProductStream = WayangCollections.streamedCrossProduct(matrix);
        final List<List<Integer>> crossProduct =
                StreamSupport.stream(crossProductStream.spliterator(), false).collect(Collectors.toList());

        List<List<Integer>> expectedCrossProduct = Arrays.asList(
                Arrays.asList(1, 4),
                Arrays.asList(2, 4),
                Arrays.asList(3, 4),
                Arrays.asList(1, 5),
                Arrays.asList(2, 5),
                Arrays.asList(3, 5),
                Arrays.asList(1, 6),
                Arrays.asList(2, 6),
                Arrays.asList(3, 6)
        );

        Assert.assertEquals(expectedCrossProduct, crossProduct);
    }

    @Test
    public void test3x2() {
        List<List<Integer>> matrix = Arrays.asList(
                Arrays.asList(1, 4),
                Arrays.asList(2, 5),
                Arrays.asList(3, 6)
        );
        final Iterable<List<Integer>> crossProductStream = WayangCollections.streamedCrossProduct(matrix);
        final List<List<Integer>> crossProduct =
                StreamSupport.stream(crossProductStream.spliterator(), false).collect(Collectors.toList());

        List<List<Integer>> expectedCrossProduct = Arrays.asList(
                Arrays.asList(1, 2, 3),
                Arrays.asList(4, 2, 3),
                Arrays.asList(1, 5, 3),
                Arrays.asList(4, 5, 3),
                Arrays.asList(1, 2, 6),
                Arrays.asList(4, 2, 6),
                Arrays.asList(1, 5, 6),
                Arrays.asList(4, 5, 6)
        );

        Assert.assertEquals(expectedCrossProduct, crossProduct);
    }

    @Test
    public void test1x3() {
        List<List<Integer>> matrix = Arrays.asList(
                Arrays.asList(1),
                Arrays.asList(2),
                Arrays.asList(3)
        );
        final Iterable<List<Integer>> crossProductStream = WayangCollections.streamedCrossProduct(matrix);
        final List<List<Integer>> crossProduct =
                StreamSupport.stream(crossProductStream.spliterator(), false).collect(Collectors.toList());

        List<List<Integer>> expectedCrossProduct = Arrays.asList(
                Arrays.asList(1, 2, 3)
        );

        Assert.assertEquals(expectedCrossProduct, crossProduct);
    }

    @Test
    public void test3x1() {
        List<List<Integer>> matrix = Arrays.asList(
                Arrays.asList(1, 2, 3)
        );
        final Iterable<List<Integer>> crossProductStream = WayangCollections.streamedCrossProduct(matrix);
        final List<List<Integer>> crossProduct =
                StreamSupport.stream(crossProductStream.spliterator(), false).collect(Collectors.toList());

        List<List<Integer>> expectedCrossProduct = Arrays.asList(
                Arrays.asList(1),
                Arrays.asList(2),
                Arrays.asList(3)
        );

        Assert.assertEquals(expectedCrossProduct, crossProduct);
    }

    @Test
    public void test1x1() {
        List<List<Integer>> matrix = Arrays.asList(
                Arrays.asList(1)
        );
        final Iterable<List<Integer>> crossProductStream = WayangCollections.streamedCrossProduct(matrix);
        final List<List<Integer>> crossProduct =
                StreamSupport.stream(crossProductStream.spliterator(), false).collect(Collectors.toList());

        List<List<Integer>> expectedCrossProduct = Arrays.asList(
                Arrays.asList(1)
        );

        Assert.assertEquals(expectedCrossProduct, crossProduct);
    }

    @Test
    public void test0x0() {
        List<List<Integer>> matrix = Arrays.asList(
        );
        final Iterable<List<Integer>> crossProductStream = WayangCollections.streamedCrossProduct(matrix);
        final List<List<Integer>> crossProduct =
                StreamSupport.stream(crossProductStream.spliterator(), false).collect(Collectors.toList());

        List<List<Integer>> expectedCrossProduct = Arrays.asList(
        );

        Assert.assertEquals(expectedCrossProduct, crossProduct);
    }

    @Test
    public void test2and0() {
        List<List<Integer>> matrix = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList()
        );
        final Iterable<List<Integer>> crossProductStream = WayangCollections.streamedCrossProduct(matrix);
        final List<List<Integer>> crossProduct =
                StreamSupport.stream(crossProductStream.spliterator(), false).collect(Collectors.toList());

        List<List<Integer>> expectedCrossProduct = Arrays.asList(
        );

        Assert.assertEquals(expectedCrossProduct, crossProduct);
    }
}
