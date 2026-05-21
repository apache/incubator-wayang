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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;

import java.util.Arrays;
import java.util.Optional;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Set;

public class TreeDecoder {

    @SuppressWarnings("unused")
    private static final Logger logger = LogManager.getLogger(TreeDecoder.class);

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
            () -> new WayangException("Couldnt recover sink operator during decoding")
        );

        Operator definitiveSink = sink;

        if (definitiveSink.isSink()) {
            return new WayangPlan(definitiveSink);
        } else {
            throw new WayangException("Recovered sink operator is not a sink");
        }
    }

    public static WayangPlan decode(TreeNode node) {
        updateOperatorPlatforms(node);

        final Operator sink = OneHotMappings.getOperatorFromEncoding(node.encoded).orElseThrow(
            () -> new WayangException("Couldnt recover sink operator during decoding")
        );

        Operator definitiveSink = sink;

        if (definitiveSink.isSink()) {
            return new WayangPlan(definitiveSink);
        } else {
            throw new WayangException("Recovered sink operator is not a sink");
        }
    }


    private static void updateOperatorPlatforms(TreeNode node) {
        /*
        if (Arrays.equals(node.encoded, OneHotEncoder.encodeNullOperator())) {
            System.out.println("1: No update of platforms on null operators");
            return;
        }*/

        if (node.isNullOperator()){
            return;
        }

        final Optional<Operator> operator = OneHotMappings.getOperatorFromEncoding(node.encoded);

        if (operator.isPresent()) {
            Platform platform = OneHotMappings.getOperatorPlatformFromEncoding(node.encoded).orElseThrow(
                () -> new WayangException(String.format("Couldnt recover platform for operator: %s with encoding %s", operator.get(), Arrays.toString(node.encoded)))
            );
            Set<Platform> platforms = operator.get().getTargetPlatforms();

            //if (platforms.size() == 0 || platforms.contains(platform)) {
                operator.get().addTargetPlatform(platform);
            //}
            //

            Collection<Operator> children = Stream.of(operator.get().getAllInputs())
                .filter(input -> input.getOccupant() != null)
                .map(input -> input.getOccupant().getOwner())
                .collect(Collectors.toList());

        } else {
            logger.info("Operator couldn't be recovered, potentially conversion operator: {}", node);

            Platform platform = OneHotMappings.getOperatorPlatformFromEncoding(node.encoded).orElseThrow(
                () -> new WayangException(String.format("Couldnt recover platform with encoding %s", Arrays.toString(node.encoded)))
            );
        }

        if (node.left != null) {
            updateOperatorPlatforms(TreeNode.class.cast(node.left));
        }

        if (node.right != null) {
            updateOperatorPlatforms(TreeNode.class.cast(node.right));
        }
    }
}
