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

import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Platform;

public class TreeDecoder {
    private static final Logger logger = LogManager.getLogger(TreeDecoder.class);
    private final OneHotMappings mappings;

    public TreeDecoder(final TreeEncoder encoder) {
        this.mappings = encoder.getMappings();
    }

    public WayangPlan decode(final String encoded) {
        final TreeNode node = TreeNode.fromString(encoded);

        updateOperatorPlatforms(node);

        final Operator sink = mappings.getOperatorFromEncoding(node.encoded)
                .orElseThrow(() -> new WayangException("Couldnt recover sink operator during decoding"));

        final Operator definitiveSink = sink;

        if (definitiveSink.isSink()) {
            return new WayangPlan(definitiveSink);
        } else {
            throw new WayangException("Recovered sink operator is not a sink");
        }
    }

    public WayangPlan decode(final TreeNode node) {
        updateOperatorPlatforms(node);

        final Operator sink = mappings.getOperatorFromEncoding(node.encoded)
                .orElseThrow(() -> new WayangException("Couldnt recover sink operator during decoding"));

        final Operator definitiveSink = sink;

        if (definitiveSink.isSink()) {
            return new WayangPlan(definitiveSink);
        } else {
            throw new WayangException("Recovered sink operator is not a sink");
        }
    }

    private void updateOperatorPlatforms(final TreeNode node) {
        if (node.isNullOperator()) {
            return;
        }

        final Optional<Operator> operator = mappings.getOperatorFromEncoding(node.encoded);

        if (operator.isPresent()) {
            final Platform platform = OneHotMappings.getOperatorPlatformFromEncoding(node.encoded)
                    .orElseThrow(() -> new WayangException(
                            String.format("Couldnt recover platform for operator: %s with encoding %s", operator.get(),
                                    Arrays.toString(node.encoded))));

            operator.get().addTargetPlatform(platform);
        } else {
            logger.info("Operator couldn't be recovered, potentially conversion operator: {}", node);
        }

        if (node.left != null) {
            updateOperatorPlatforms(TreeNode.class.cast(node.left));
        }

        if (node.right != null) {
            updateOperatorPlatforms(TreeNode.class.cast(node.right));
        }
    }
}
 