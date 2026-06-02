/*
 *
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

package org.apache.wayang.ml.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.wayang.ml.encoding.TreeNode;
import org.junit.jupiter.api.Test;

public class TreeNodeTest {
    @Test
    public void testEncodingFromString() throws IOException, URISyntaxException {
        String encoded = "((0,1,2,3),((4,5,6,7), ((8,9,10,11),((12,13,14,15),((16,17,18,19),((20,21,22,23),((24,25,26,27),),((28,29,30,31),)),((32,33,34,35),)),((36,37,38,39),)),((40,41,42,43),)),((44,45,46,47),)),((48,49,50,51),))";
        encoded = encoded.replaceAll("\\s+", "");
        final TreeNode decoded = TreeNode.fromString(encoded);

        assertEquals(encoded, decoded.toStringEncoding());
    }
}
