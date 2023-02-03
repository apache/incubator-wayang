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

package org.apache.wayang.agoraeo.patches;

import org.apache.wayang.basic.data.Tuple2;
import org.gdal.gdal.*;
import org.gdal.osr.SpatialReference;

import java.io.Serializable;

public class BandMetadata implements Serializable {


    private final String utm;
    private final String resolution;
    private final String band_name;

    /*TODO: Band raster is just the first of the bands of the raster, I don't know how representative it is*/
    private final org.gdal.gdal.Band band_raster;
    private final org.gdal.gdal.Dataset band_source;
    private final String local_path;
    private final Tuple2<Double, Double> ul;
    private final Tuple2<Double, Double> lr;
    private final Tuple2<Double, Double> pixel_resolution;
    private final Tuple2<Integer, Integer> size;
    private final Integer espg;
    private final String projection;

    public BandMetadata(Band b) {
        gdal.AllRegister();
        this.utm = b.getUtm();
        this.band_name = b.getBand_name();
        this.resolution = b.getResolution();
        this.local_path = b.getBand_path();
        this.band_source = gdal.Open(b.getBand_path());
        this.size = new Tuple2<>(this.band_source.GetRasterXSize(), this.band_source.getRasterYSize());
        double[] geo_transf = this.band_source.GetGeoTransform();
        this.pixel_resolution = new Tuple2<>(geo_transf[1], geo_transf[5]);
        this.ul = new Tuple2<>(geo_transf[0], geo_transf[3]);
        this.lr = new Tuple2<>(
                this.ul.field0 + (this.pixel_resolution.field0 * this.size.field0),
                this.ul.field1 + (this.pixel_resolution.field1 * this.size.field1)
        );
        this.projection = this.band_source.GetProjection();
        this.band_raster = this.band_source.GetRasterBand(1);
        String code_espg = new SpatialReference(this.projection).GetAttrValue("AUTHORITY", 1);
        this.espg = checkCode(code_espg);
    }

    private Integer checkCode(String code_espg) {

        int intValue;

        if(code_espg == null || code_espg.equals("")) {
            return -1;
        }

        try {
            intValue = Integer.parseInt(code_espg);
            return intValue;
        } catch (NumberFormatException e) {
            System.err.println("Not possible to transform " + code_espg + " to Integer");
        }
        return -1;
    }

    public org.gdal.gdal.Band getBand_raster() {
        return band_raster;
    }

    public Dataset getBand_source() {
        return band_source;
    }

    public String getLocal_path() {
        return local_path;
    }

    public Tuple2<Double, Double> getUl() {
        return ul;
    }

    public Tuple2<Double, Double> getLr() {
        return lr;
    }

    public Tuple2<Double, Double> getPixel_resolution() {
        return pixel_resolution;
    }

    public Tuple2<Integer, Integer> getSize() {
        return size;
    }

    public Integer getEspg() {
        return espg;
    }

    public String getProjection() {
        return projection;
    }

    @Override
    public String toString() {
        return
            "utm: " + this.utm + " | " +
            "resolution: " + this.resolution + " | " +
            "band_name: " + this.band_name + " | " +
            "band_path: " + this.local_path.substring(this.local_path.lastIndexOf("/")+1) + " :  " +
            "espg: " + this.espg + " | " +
            "pixel_resolution: " + this.pixel_resolution + " | " +
            "ul: " + this.ul + " | " +
            "lr: " + this.lr + " | " +
            "size: " + this.size
//            this.projection + "\n"
                ;
    }
}
