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

package org.apache.wayang.ml.encoding;

import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.java.Java;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class TreeDecoder {


    /**
     * Plan:
     *  - retrieve OneHotMappings
     *  - iterate through operators and recreate basic wayang operators
     *  - use OneHotMappings to addTargetPlatform on basic operator
     *  - connect operator slots recursively after creating left and right children
     *  - return new WayangPlan(sink)
     */
    public static WayangPlan decode(String encoded) {
        TreeNode node = TreeNode.fromString(encoded);

        updateOperatorPlatforms(node);

        final Operator sink = OneHotMappings.getOperatorFromEncoding(node.encoded).orElseThrow(
            () -> new WayangException("Couldnt recover operator during decoding")
        );

        Operator definitiveSink = sink;

        if (definitiveSink.isSink()) {
            return new WayangPlan(definitiveSink);
        } else {
            throw new WayangException("Recovered sink operator is not a sink");
        }
    }

    private static void updateOperatorPlatforms(TreeNode node) {
        if (Arrays.equals(node.encoded, OneHotEncoder.encodeNullOperator())) {
            return;
        }

        final Operator operator = OneHotMappings.getOperatorFromEncoding(node.encoded).orElseThrow(
            () -> new WayangException("Couldnt recover operator during decoding")
        );
        Platform platform = OneHotMappings.getOperatorPlatformFromEncoding(node.encoded).orElseThrow(
            () -> new WayangException(String.format("Couldnt recover platform for operator: %s", operator))
        );

        operator.addTargetPlatform(platform);

        if (node.left != null) {
            updateOperatorPlatforms(node.left);
        }

        if (node.right != null) {
            updateOperatorPlatforms(node.right);
        }
    }
}
