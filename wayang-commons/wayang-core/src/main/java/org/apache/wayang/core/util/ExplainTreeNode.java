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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.core.util.json.WayangJsonArray;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExplainTreeNode implements JsonSerializable {
    public Operator operator;
    public List<ExplainTreeNode> children = new ArrayList<>();

    @Override
    public WayangJsonObj toJson() {
        WayangJsonObj result = new WayangJsonObj();

        if (this.operator instanceof OperatorAlternative) {
            OperatorAlternative alts = (OperatorAlternative) this.operator;
            result.put(
                "name",
                alts.getAlternatives()
                    .get(0)
                    .getContainedOperators()
                    .iterator()
                    .next()
                    .getClass()
                    .getSimpleName()
            );
        } else {
            result.put("name", this.operator.getClass().getSimpleName());
        }

        if (this.operator instanceof ExecutionOperator) {
            result.put("platform", ((ExecutionOperator) this.operator).getPlatform().getName());
        }

        if (!operator.isSource() && !operator.isSink() && operator.getName() != null) {
            WayangJsonObj attributes = new WayangJsonObj();
            attributes.put("operation", operator.getName());
            result.put("attributes", attributes);
        }

        if (this.children.size() > 0) {
            final WayangJsonArray jsonChildren = new WayangJsonArray();
            this.children.stream().forEach(child -> {
                jsonChildren.put(child.toJson());
            });
            result.put("children", jsonChildren);
        }

        return result;
    }

    @SuppressWarnings("unused")
    public static ExplainTreeNode fromJson(WayangJsonObj obj) {
        return new ExplainTreeNode();
    }
}
