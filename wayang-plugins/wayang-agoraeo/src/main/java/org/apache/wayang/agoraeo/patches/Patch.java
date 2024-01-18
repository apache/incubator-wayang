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
    private final Tuple2<Integer, Integer> ordinalPositionOnImage;
    private final Integer espg;
    private final String projection;
    private final byte[] data;
    private final Tuple2<Double, Double> pixel_resolution;

    public Patch(String utm, String band_resolution, String band_name, Double ulx, Double uly, Double lrx, Double lry, Tuple2<Integer, Integer> ordinalPositionOnImage, Integer espg, String projection, byte[] data, Tuple2<Double, Double> pixel_resolution) {
        this.utm = utm;
        this.band_resolution = band_resolution;
        this.band_name = band_name;
        this.ulx = ulx;
        this.uly = uly;
        this.lrx = lrx;
        this.lry = lry;
        this.ordinalPositionOnImage = ordinalPositionOnImage;
        this.espg = espg;
        this.projection = projection;
        this.data = data;
        this.pixel_resolution = pixel_resolution;
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

    public byte[] getData() {
        return data;
    }

    public Tuple2<Double, Double> getPixel_resolution() {
        return pixel_resolution;
    }

    public Tuple2<Integer, Integer> getOrdinalPositionOnImage() {
        return ordinalPositionOnImage;
    }

    public Integer getEspg() {
        return espg;
    }

    public String getProjection() {
        return projection;
    }

    public String oldToString() {
        return "8281 Patchs to be generated for{" +
                "utm='" + utm + '\'' +
                ", band_resolution='" + band_resolution + '\'' +
                ", band_name='" + band_name + '\'' +
                ", pixel_resolution=" + pixel_resolution +
                '}';
    }

    @Override
    public String toString() {
        return utm + '|' + band_resolution + '|' + band_name + '|' +
        ulx + "," + uly + "," + lrx + "," + lry + "|" + data.length;
    }
}
