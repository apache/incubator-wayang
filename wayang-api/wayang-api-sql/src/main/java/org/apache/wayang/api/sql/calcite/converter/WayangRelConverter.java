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
 *
 */

package org.apache.wayang.api.sql.calcite.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.wayang.api.sql.calcite.rel.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangRelConverter {
    private final Configuration configuration;

    public WayangRelConverter(final Configuration configuration) {
        this.configuration = configuration;
    }

    public WayangRelConverter() {
        this.configuration = null;
    }

    /**
     * Some visitors may rely on configuration like the
     * {@link WayangTableScanVisitor}, that uses it
     * to specify its calcite schema when fetching from files on disk
     * 
     * @return {@link Configuration}, or null if {@link WayangRelConverter} is not
     *         constructed with one.
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    public Operator convert(final RelNode node) {
        if (node instanceof WayangTableScan) {
            return new WayangTableScanVisitor(this).visit((WayangTableScan) node);
        } else if (node instanceof WayangProject) {
            return new WayangProjectVisitor(this).visit((WayangProject) node);
        } else if (node instanceof WayangFilter) {
            return new WayangFilterVisitor(this).visit((WayangFilter) node);
        } else if (node instanceof WayangJoin) {
            return new WayangJoinVisitor(this).visit((WayangJoin) node);
        } else if (node instanceof WayangAggregate) {
            return new WayangAggregateVisitor(this).visit((WayangAggregate) node);
        }
        throw new IllegalStateException("Operator translation not supported yet");
    }
}
