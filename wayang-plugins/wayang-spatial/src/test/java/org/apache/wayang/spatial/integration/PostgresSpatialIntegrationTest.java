/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.spatial.integration;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Disabled("Requires local Postgres test database.")
public class PostgresSpatialIntegrationTest {


    public static void main(String[] args) {
        WayangPlan wayangPlan;
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/imdb");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "password");

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        Collection<Record> collector = new ArrayList<>();

        TableSource customer = new PostgresTableSource("person");
        MapOperator<Record, Record> projection = MapOperator.createProjection(
                Record.class,
                Record.class,
                "name");

        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);
        customer.connectTo(0,projection,0);
        projection.connectTo(0,sink,0);


        wayangPlan = new WayangPlan(sink);

        wayangContext.execute("PostgreSql test", wayangPlan);


        int count = 10;
        for(Record r : collector) {
            System.out.println(r.getField(0).toString());
            if(--count == 0 ) {
                break;
            }
        }
        System.out.println("Done");
    }

    WayangContext getTestWayangContext() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5433/postgres"); // Default port 5432
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

        return new WayangContext(configuration);
    }

    @Test
    @Disabled("Requires local Postgres test database.")
    void testSpatialFilterOperator() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        ///  Scalar Geometry
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.4, 0.00, 0.40);
        Geometry geom2 = geometryFactory.toGeometry(envelope);

        TableSource spider =
                new PostgresTableSource("spider_boxes", "id", "geom");

        SpatialFilterOperator<Record> spatialFilterOperator = new SpatialFilterOperator<Record>(
                SpatialPredicate.INTERSECTS,
                (record -> (WayangGeometry.fromStringInput(record.getString(1)))),
                DataSetType.createDefaultUnchecked(Record.class),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.4 0.00,0.4 0.4,0.00 0.4,0.00 0.00))"));

        spatialFilterOperator.getKeyDescriptor().withSqlImplementation("spatialdb", "geom");
        spatialFilterOperator.addTargetPlatform(Spark.platform());
        spider.connectTo(0,spatialFilterOperator,0);

        Collection<Tuple2<Integer, WayangGeometry>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Integer, WayangGeometry>> sink
                = LocalCallbackSink.createCollectingSink(collector, DataSetType.createDefaultUnchecked(Record.class));
        spatialFilterOperator.connectTo(0, sink, 0);

        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);

        assertEquals(19, collector.size());
    }

    @Test
    @Disabled("Requires local Postgres test database.")
    void testSpatialFilterWithTuple() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        ///  Scalar Geometry
        GeometryFactory geometryFactory = new GeometryFactory();
        Envelope envelope = new Envelope(0.00, 0.4, 0.00, 0.40);
        Geometry geom2 = geometryFactory.toGeometry(envelope);

        TableSource spider =
                new PostgresTableSource("spider", "id", "geom");

        MapOperator<Record, Tuple2<Integer, WayangGeometry>> mapToTuple = new MapOperator<Record, Tuple2<Integer, WayangGeometry>>(
                record -> {
                    Tuple2<Integer, WayangGeometry> tuple = new Tuple2<>();
                    tuple.field0 = record.getInt(0);
                    tuple.field1 = WayangGeometry.fromStringInput(record.getField(1).toString());
                    return tuple;
                },
                Record.class,
                ReflectionUtils.specify(Tuple2.class)
        );

        SpatialFilterOperator<Tuple2<Integer, WayangGeometry>> spatialFilterOperator = new SpatialFilterOperator<Tuple2<Integer, WayangGeometry>>(
                SpatialPredicate.INTERSECTS,
                Tuple2::getField1,
                DataSetType.createDefaultUnchecked(Tuple2.class),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.4 0.00,0.4 0.4,0.00 0.4,0.00 0.00))"));

        spatialFilterOperator.addTargetPlatform(Java.platform());
        spider.connectTo(0,mapToTuple,0);
        mapToTuple.connectTo(0,spatialFilterOperator,0);

        Collection<Tuple2<Integer, WayangGeometry>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Integer, WayangGeometry>> sink
                = LocalCallbackSink.createCollectingSink(collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialFilterOperator.connectTo(0, sink, 0);

        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);
        assertEquals(19, collector.size());
    }

    @Test
    @Disabled("Requires local Postgres test database.")
    void testSpatialJoin() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        TableSource table1 = new PostgresTableSource("spider_boxes", "id", "x_min", "y_min", "x_max", "y_max", "geom");

        // Input polygons: nested axis-aligned squares.
        final List<WayangGeometry> inputValues = Arrays.asList(
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.40 0.00,0.40 0.40,0.00 0.40,0.00 0.00))"),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.30 0.00,0.30 0.30,0.00 0.30,0.00 0.00))"),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.20 0.00,0.20 0.20,0.00 0.20,0.00 0.00))"),
                WayangGeometry.fromStringInput("POLYGON((0.00 0.00,0.10 0.00,0.10 0.10,0.00 0.10,0.00 0.00))")
        );
        CollectionSource<WayangGeometry> inputCollection = new CollectionSource<>(inputValues, WayangGeometry.class);


        SpatialJoinOperator<Record, WayangGeometry> spatialJoinOperator = new SpatialJoinOperator<>(
                record -> WayangGeometry.fromStringInput(record.getString(4)),
                wgeometry -> wgeometry,
                Record.class, WayangGeometry.class,
                SpatialPredicate.INTERSECTS
                );
        table1.connectTo(0, spatialJoinOperator, 0);
        inputCollection.connectTo(0, spatialJoinOperator, 1);

        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink
                = LocalCallbackSink.createCollectingSink(collector, DataSetType.createDefaultUnchecked(Tuple2.class));
        spatialJoinOperator.connectTo(0, sink, 0);
        wayangContext.execute("PostgreSql test", new WayangPlan(sink));

        System.out.println(collector);

        assertEquals(30, collector.size());
    }

    @Test
    @Disabled("Requires local Postgres test database.")
    void testSpatialJoinDbSources() {
        WayangContext wayangContext = getTestWayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        // Two logical sources over the same table.
        TableSource table1 = new PostgresTableSource("spider_boxes",  "id", "x_min", "y_min", "x_max", "y_max", "geom");
        TableSource table2 = new PostgresTableSource("spider_boxes",  "id", "x_min", "y_min", "x_max", "y_max", "geom");

        // Spatial join on INTERSECTS; both sides use the geom column (index 5).
        SpatialJoinOperator<Record, Record> spatialJoinOperator =
                new SpatialJoinOperator<>(
                        record -> WayangGeometry.fromStringInput(record.getString(5)),
                        record -> WayangGeometry.fromStringInput(record.getString(5)),
                        Record.class, Record.class,
                        SpatialPredicate.INTERSECTS
                );

        // Register SQL implementations for both inputs
        spatialJoinOperator.getKeyDescriptor0()
                .withSqlImplementation("spiderdb", "geom");
        spatialJoinOperator.getKeyDescriptor1()
                .withSqlImplementation("spiderdb", "geom");

        spatialJoinOperator.addTargetPlatform(Postgres.platform());

        // Wire up both DB sources as inputs to the spatial join.
        table1.connectTo(0, spatialJoinOperator, 0);
        table2.connectTo(0, spatialJoinOperator, 1);

        // Collect results.
        Collection<Tuple2<Record, Record>> collector = new ArrayList<>();
        LocalCallbackSink<Tuple2<Record, Record>> sink =
                LocalCallbackSink.createCollectingSink(
                        collector,
                        DataSetType.createDefaultUnchecked(Tuple2.class)
                );
        spatialJoinOperator.connectTo(0, sink, 0);

        // Execute the plan.
        wayangContext.execute("PostgreSql spatial join DB-DB", new WayangPlan(sink));

        // Basic sanity check: we should get at least self-intersections.
        assertFalse(collector.isEmpty(), "Spatial join result should not be empty.");

        // Semantic check: every returned pair must actually intersect according to JTS.
        for (Tuple2<Record, Record> pair : collector) {
            Geometry g1 = WayangGeometry.fromStringInput(pair.field0.getString(1)).getGeometry();
            Geometry g2 = WayangGeometry.fromStringInput(pair.field1.getString(1)).getGeometry();
            assertTrue(
                    g1.intersects(g2),
                    "Found non-intersecting pair in spatial join result."
            );
        }
    }
}
