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

package org.apache.wayang.spatial.data;

import org.apache.wayang.spatial.data.WayangGeometry;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;

import static org.junit.Assert.*;

public class WayangGeometryTest {

    private final GeometryFactory gf = new GeometryFactory();

    @Test
    public void testFromGeometryStoresAndCachesGeometry() {
        Point point = gf.createPoint(new Coordinate(1.0, 2.0));

        WayangGeometry wGeometry = WayangGeometry.fromGeometry(point);

        // First call should give us exactly the same instance
        Geometry first = wGeometry.getGeometry();
        assertSame("Geometry instance should be the same as the one passed in.",
                point, first);

        // Second call should return the same cached instance
        Geometry second = wGeometry.getGeometry();
        assertSame("Geometry instance should be cached and reused.", first, second);

        // Derived representations should be non-null / non-empty
        String wkt = wGeometry.getWKT();
        String wkb = wGeometry.getWKB();
        String geoJson = wGeometry.getGeoJSON();

        assertNotNull("WKT should not be null.", wkt);
        assertFalse("WKT should not be empty.", wkt.isEmpty());
        assertNotNull("WKB should not be null.", wkb);
        assertFalse("WKB should not be empty.", wkb.isEmpty());
        assertNotNull("GeoJSON should not be null.", geoJson);
        assertFalse("GeoJSON should not be empty.", geoJson.isEmpty());
    }

    @Test
    public void testFromStringInputWKTAndSRIDCleaning() {
        // WKT with SRID prefix
        String wktWithSrid = "SRID=4326;POINT (1 2)";
        WayangGeometry wGeometry = WayangGeometry.fromStringInput(wktWithSrid);

        Geometry geom = wGeometry.getGeometry();
        assertTrue("Geometry should be a Point.", geom instanceof Point);
        Point p = (Point) geom;
        assertEquals(1.0, p.getX(), 1e-9);
        assertEquals(2.0, p.getY(), 1e-9);

        // getWKT returns the original stored WKT, including SRID
        String wkt = wGeometry.getWKT();
        assertTrue("Original WKT (with SRID) should be preserved.", wkt.startsWith("SRID="));

        // Verify that parsing the same WKT without SRID gives an equal geometry,
        // which indirectly asserts that cleanSRID() worked as expected.
        String wktWithoutSrid = "POINT (1 2)";
        WayangGeometry wGeometryNoSrid = WayangGeometry.fromStringInput(wktWithoutSrid);
        Geometry geomNoSrid = wGeometryNoSrid.getGeometry();

        assertTrue("Geometry from SRID-prefixed WKT should equal geometry from plain WKT.",
                geom.equalsExact(geomNoSrid));
    }


    @Test
    public void testFromStringInputPlainWKT() {
        // Use JTS writer to generate canonical WKT string
        Point point = gf.createPoint(new Coordinate(3.0, 4.0));
        String canonicalWkt = new WKTWriter().write(point);

        WayangGeometry wGeometry = WayangGeometry.fromStringInput(canonicalWkt);

        Geometry geom = wGeometry.getGeometry();
        assertTrue(geom instanceof Point);
        assertEquals(point.getCoordinate().x, geom.getCoordinate().x, 1e-9);
        assertEquals(point.getCoordinate().y, geom.getCoordinate().y, 1e-9);

        // getWKT should match the canonical representation from JTS
        String wkt = wGeometry.getWKT();
        assertEquals("WKT should match JTS canonical representation.", canonicalWkt, wkt);
    }

    @Test
    public void testFromStringInputWKBHexRoundTrip() {
        Point original = gf.createPoint(new Coordinate(5.0, 6.0));

        // Encode to WKB hex using same mechanism as WayangGeometry
        WKBWriter wkbWriter = new WKBWriter();
        byte[] wkbBytes = wkbWriter.write(original);
        String wkbHex = WKBWriter.toHex(wkbBytes);

        WayangGeometry wGeometry = WayangGeometry.fromStringInput(wkbHex);
        Geometry parsed = wGeometry.getGeometry();

        assertTrue("Parsed geometry should be a Point.", parsed instanceof Point);
        assertTrue("Parsed geometry should be exactly equal to original.",
                original.equalsExact(parsed));

        // getWKB should give back a hex string that decodes to the same WKB bytes
        String producedHex = wGeometry.getWKB();
        byte[] producedBytes = WKBReader.hexToBytes(producedHex);
        assertArrayEquals("WKB bytes should be identical after round-trip.",
                wkbBytes, producedBytes);
    }

    @Test
    public void testFromStringInputGeoJSONAndRoundTripThroughGeometry() {
        // Simple GeoJSON Point
        String geoJson = "{\"type\":\"Point\",\"coordinates\":[7.0,8.0]}";

        WayangGeometry wGeometry = WayangGeometry.fromStringInput(geoJson);
        Geometry geom = wGeometry.getGeometry();

        assertTrue("Geometry should be a Point.", geom instanceof Point);
        Point p = (Point) geom;
        assertEquals(7.0, p.getX(), 1e-9);
        assertEquals(8.0, p.getY(), 1e-9);

        // Now go back through fromGeometry + GeoJSON
        WayangGeometry fromGeom = WayangGeometry.fromGeometry(geom);
        String generatedGeoJson = fromGeom.getGeoJSON();

        // We don't depend on exact string equality/ordering of JSON,
        // but we do expect that parsing generated GeoJSON yields an equal geometry.
        WayangGeometry reParsed = WayangGeometry.fromStringInput(generatedGeoJson);
        Geometry geom2 = reParsed.getGeometry();

        assertTrue("Geometry from re-parsed GeoJSON should be exactly equal.",
                geom.equalsExact(geom2));
    }

    @Test
    public void testPreferredRepresentationOrderWktThenWkbThenGeoJson() {
        // Start with WKT-only instance
        Point point = gf.createPoint(new Coordinate(10.0, 20.0));
        String wkt = new WKTWriter().write(point);
        WayangGeometry wFromWkt = WayangGeometry.fromStringInput(wkt);

        Geometry g1 = wFromWkt.getGeometry();
        assertTrue(g1 instanceof Point);
        assertEquals(point.getX(), g1.getCoordinate().x, 1e-9);
        assertEquals(point.getY(), g1.getCoordinate().y, 1e-9);

        // Now WKB-only instance
        WKBWriter wkbWriter = new WKBWriter();
        byte[] wkbBytes = wkbWriter.write(point);
        String wkbHex = WKBWriter.toHex(wkbBytes);
        WayangGeometry wFromWkb = WayangGeometry.fromStringInput(wkbHex);

        Geometry g2 = wFromWkb.getGeometry();
        assertTrue(g2 instanceof Point);
        assertTrue(point.equalsExact(g2));

        // And GeoJSON-only instance
        WayangGeometry wFromGeo = WayangGeometry.fromGeometry(point);
        String geoJson = wFromGeo.getGeoJSON();
        WayangGeometry wFromGeoOnly = WayangGeometry.fromStringInput(geoJson);

        Geometry g3 = wFromGeoOnly.getGeometry();
        assertTrue(g3 instanceof Point);
        assertTrue(point.equalsExact(g3));
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidWKTThrowsRuntimeException() {
        // This should cause JTS WKTReader to throw ParseException,
        // which WayangGeometry wraps in a RuntimeException.
        String invalidWkt = "POINT (1)";
        WayangGeometry wGeometry = WayangGeometry.fromStringInput(invalidWkt);

        // Should throw
        wGeometry.getGeometry();
    }

    @Test(expected = IllegalStateException.class)
    public void testNoRepresentationAvailableThrowsIllegalStateException() {
        // Default constructor, no wkt/wkb/geojson/geometry set
        WayangGeometry wGeometry = new WayangGeometry();

        // Should hit the "No geometry representation available" branch
        wGeometry.getGeometry();
    }

    @Test
    public void testGetGeometryIsCached() {
        Point point = gf.createPoint(new Coordinate(11.0, 22.0));
        WayangGeometry wGeometry = WayangGeometry.fromGeometry(point);

        Geometry g1 = wGeometry.getGeometry();
        Geometry g2 = wGeometry.getGeometry();

        assertSame("getGeometry should cache and return the same instance.", g1, g2);
    }
}
