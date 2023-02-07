package org.apache.wayang.agoraeo.patches;

import org.apache.wayang.basic.data.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class Patch implements Serializable {

    private final String utm;
    private final String band_resolution;
    private final String band_name;
    private final Double ulx;
    private final Double uly;
    private final Double lrx;
    private final Double lry;
    private final ByteBuffer data;
    private final Tuple2<Double, Double> pixel_resolution;

    public Patch(String utm, String band_resolution, String band_name, Double ulx, Double uly, Double lrx, Double lry, ByteBuffer data, Tuple2<Double, Double> pixel_resolution) {
        this.utm = utm;
        this.band_resolution = band_resolution;
        this.band_name = band_name;
        this.ulx = ulx;
        this.uly = uly;
        this.lrx = lrx;
        this.lry = lry;
        this.data = data;
        this.pixel_resolution = pixel_resolution;
    }

    public Patch(BandMetadata band){
        this.utm = null;
        this.band_resolution = null;
        this.band_name = null;
        this.ulx = null;
        this.uly = null;
        this.lrx = null;
        this.lry = null;
        this.data = null;
        this.pixel_resolution = null;
    }

    public String getUtm() {
        return utm;
    }

    public String getBand_resolution() {
        return band_resolution;
    }

    public String getBand_name() {
        return band_name;
    }

    public Double getUlx() {
        return ulx;
    }

    public Double getUly() {
        return uly;
    }

    public Double getLrx() {
        return lrx;
    }

    public Double getLry() {
        return lry;
    }

    public ByteBuffer getData() {
        return data;
    }

    public Tuple2<Double, Double> getPixel_resolution() {
        return pixel_resolution;
    }
}
