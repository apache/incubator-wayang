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

import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.java.Java;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spatial.Spatial;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.basic.data.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class SpatialJoinPostgis {
    public static void main(String[] args) {
        System.out.println("Running Spatial Join Benchmark with args " + Arrays.toString(args) + " on Postgres");

        Configuration configuration = new Configuration();

        String tableName1 = args[1];
        String tableName2 = args[2];
        String node_name = args[3];
        String database_name = args[4];
        String platform = args[5];

        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://" + node_name + ":5432/" + database_name);
        configuration.setProperty("wayang.postgres.jdbc.user", "wayang_user");
        configuration.setProperty("wayang.postgres.jdbc.password", "wayang");

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Postgres.plugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Spatial.plugin());

        TableSource table1 = new PostgresTableSource(tableName1,  "ST_AsText(geom)");
        TableSource table2 = new PostgresTableSource(tableName2,  "ST_AsText(geom)");

        SpatialJoinOperator<Record, Record> spatialJoin = new SpatialJoinOperator<>(
                (record -> WayangGeometry.fromStringInput(record.getString(0))),
                (record -> WayangGeometry.fromStringInput(record.getString(0))),
                Record.class, Record.class,
                SpatialPredicate.INTERSECTS
        );

        spatialJoin.getKeyDescriptor0().withSqlImplementation(tableName1, "geom");
        spatialJoin.getKeyDescriptor1().withSqlImplementation(tableName2, "geom");
        spatialJoin.addTargetPlatform(switch (platform) {
            case "java"  -> Java.platform();
            case "spark" -> Spark.platform();
            default       -> Postgres.platform();
        });

        table1.connectTo(0, spatialJoin, 0);
        table2.connectTo(0, spatialJoin, 1);

        CountOperator<Tuple2<Record, Record>> count = new CountOperator<>(
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        spatialJoin.connectTo(0, count, 0);

        Collection<Long> outputcount = new ArrayList<>();
        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                outputcount,
                DataSetType.createDefaultUnchecked(Long.class)
        );

        count.connectTo(0, sink, 0);

        wayangContext.execute("Benchmark spatial_join", new WayangPlan(sink));

        System.out.println("Spatial Join Postgres Result Size: " + outputcount);
    }
}
