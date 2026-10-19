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

package org.apache.wayang.apps.spatial;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spatial.Spatial;
import org.apache.wayang.spatial.data.WayangGeometry;

import java.util.Arrays;
import java.util.Collection;

public class SpatialFilterPostgis {
    public static void main(String[] args) {
        System.out.println("Running Spatial Filter Benchmark with args " + Arrays.toString(args) + " on Postgres");

        Configuration configuration = new Configuration();

        String tableName = args[1];
        String node_name = args[2];
        String database_name = args[3];
        String selectivity = args[4];

        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://" + node_name + ":5432/" + database_name); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "wayang_user");
        configuration.setProperty("wayang.postgres.jdbc.password", "wayang");

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spatial.plugin());

        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        SpatialGeometry queryGeometry = WayangGeometry.fromStringInput(
                "POLYGON((0.0 0.0, " + selectivity + " 0.0, " + selectivity + " " + selectivity + ", 0.0 " + selectivity + ", 0.0 0.0))"
        );

        final Collection<Long> outputcount = builder
                .readTable(new PostgresTableSource(tableName, "ST_AsText(geom)"))
                .spatialFilter(
                        (input -> WayangGeometry.fromStringInput(input.getString(0))),
                        SpatialPredicate.INTERSECTS,
                        queryGeometry
                ).withSqlGeometryColumnName("geom")
                .withTargetPlatform(Postgres.platform())
                .count()
                .collect();

        System.out.println("Spatial Filter Postgres Result Size: " + outputcount);
    }
}
