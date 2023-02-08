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

package org.apache.wayang.agoraeo;

import org.apache.wayang.agoraeo.patches.Band;
import org.apache.wayang.agoraeo.patches.BandMetadata;
import org.apache.wayang.agoraeo.patches.L2aFile;
import org.apache.wayang.agoraeo.patches.Patch;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;
import scala.Tuple4;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MakePatches {

    private static Map<String, String> resolutionMap;

    public static void main(String[] args) {

        resolutionMap = new HashMap<>();
        resolutionMap.put("B01", "60");
        resolutionMap.put("B02", "10");
        resolutionMap.put("B03", "10");
        resolutionMap.put("B04", "10");
        resolutionMap.put("B05", "20");
        resolutionMap.put("B06", "20");
        resolutionMap.put("B07", "20");
        resolutionMap.put("B08", "10");
        resolutionMap.put("B8A", "20");
        resolutionMap.put("B09", "60");
        resolutionMap.put("B10", "60");
        resolutionMap.put("B11", "20");
        resolutionMap.put("B12", "20");
        resolutionMap.put("TCI", "20");
        resolutionMap.put("AOT", "20");
        resolutionMap.put("WVP", "20");
        resolutionMap.put("SCL", "60");

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());
        wayangContext.register(WayangAgoraEO.plugin());

        Configuration config = wayangContext.getConfiguration();
        config.load(ReflectionUtils.loadResource(WayangAgoraEO.DEFAULT_CONFIG_FILE));

        System.out.println("Running Patch Making Process!");

        List<BandMetadata> result = new ArrayList<>();
        WayangPlan w = createWayangPlan(args[0], result);


        // Source with L2A Sentinel Files
        /*
        get_band_infos receive DataFrame "l2a_path_df" containing "l2a_uuid", "l2a_path" and "l1c_uuid" columns

        Defines get_band_info:
            l2a_path/GRANULE/%unique_folder%/IMG_DATA/
            you have resolutions R10m, R20m and R60m folders
            Go through all of them reading the jp2 files inside
            Splitting the name of the jp2 file by "_", obtain [0]:utm, [2]:band_name, [3]:resolution(apply regex "^\d+")
                confirm that the resolution is consistent with the band name
                    "B01": 60,
                    "B02": 10,
                    "B03": 10,
                    "B04": 10,
                    "B05": 20,
                    "B06": 20,
                    "B07": 20,
                    "B08": 10,
                    "B8A": 20,
                    "B09": 60,
                    "B10": 60,
                    "B11": 20,
                    "B12": 20,
                    "TCI": 20,
                    "AOT": 20,
                    "WVP": 20,
                    "SCL": 60,
                confirm the band name is different from exclude_bands=["TCI", "AOT", "WVP"]
            Create Dataframe.Row(band_name=band_name, band_path=band_path, resolution=resolution, utm=utm)
            fill Array of Rows appending each Row to an array "band_infos"
            In pyspark uses functions.py udf(get_band_info, schema) to create a UDF from get_band_info that returns
            schema
               schema = T.ArrayType(
                T.StructType(
                    [
                        T.StructField("band_name", T.StringType(), False),
                        T.StructField("band_path", T.StringType(), False),
                        T.StructField("resolution", T.ShortType(), False),
                        T.StructField("utm", T.StringType(), False),
                    ]
                )
            )
            udf will retrieve an Array of Dataframe.Rows with the StructType schema
            if wide: we pass a l2a_path to the UDF, and we explode the result (functions.py explode)
                F.explode(find_band_path_udf("l2a_path"))
                    I believe after reading about the function that the only difference is that
                    will create a Dataframe of schema, instead of an Array of Schema

            Projects a new Dataframe with Select. Per Row in the input Dataframe l2a_path_df, assigns a Row for the new
            Dataframe
                "l1c_uuid",
                "l2a_uuid",
                "l2a_path",
                F.explode(find_band_path_udf("l2a_path")).alias("bands")
            As explained before, I suspect that the column bands it's a Dataframe of Schema, where each element represents
            a band with its band name, path, resolution and UTM.
            * Using mode "narrow" would only pass the array of Rows, not a Dataframe
         */


        /*
         * Load_patches receives "band_info_df" Dataframe: ("l1c_uuid", "l2a_uuid", "l2a_path", F.explode(find_band_path_udf("l2a_path")).alias("bands"))
         * over band_info_df.rdd executes a flatmap(gdal_load_and_split)
         * gdal_load_and_split(band_info_row): // Describes the treatment of each row l1c_uuid
                Uses osgeo.gdal
         *
         */


        wayangContext.execute(w, ReflectionUtils.getDeclaringJar(MakePatches.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class), ReflectionUtils.getDeclaringJar(SparkPlatform.class));

//        for (BandMetadata res : result) {
//            System.out.println(res);
//        }
    }

    public static WayangPlan createWayangPlan(
            String inputFileUrl,
            List<BandMetadata> result) {

        System.out.println(inputFileUrl);

        CollectionSource<String> source = new CollectionSource<>(Arrays.asList(inputFileUrl.split(",")), String.class);

        /*Create dataframe*/
        MapOperator<String, L2aFile> l2a_catalog = new MapOperator<>(
                t -> {
                    // UUID are just generated there, so, do the same
                    UUID l1c_uuid = UUID.randomUUID();
                    UUID l2a_uuid = UUID.randomUUID();
                    return new L2aFile(l2a_uuid.toString(), t, l1c_uuid.toString());
                },
                String.class,
                L2aFile.class
        );

        /* TODO: BigEarthNet Pipeline */
        FlatMapOperator<L2aFile, Band> bands = new FlatMapOperator<>(
                t -> {
                    List<File> paths = Stream.of(Objects.requireNonNull(new File(t.getL2a_path() + "/GRANULE").listFiles()))
                            .filter(File::isDirectory)
                            .collect(Collectors.toList());

                    if(paths.size() > 1) {
                        throw new WayangException("Granule is expected to have a unique folder inside");
                    }

                    String img_folder = paths.get(0).getAbsolutePath() + "/IMG_DATA";

                    List<Band> bands_resolution = processResolution(img_folder, "R10m", t);
                    bands_resolution.addAll(processResolution(img_folder, "R20m", t));
                    bands_resolution.addAll(processResolution(img_folder, "R60m", t));

                    return bands_resolution;
                },
                L2aFile.class,
                Band.class
        );

        MapOperator<Band, BandMetadata> metadata = new MapOperator<>(BandMetadata::new, Band.class,BandMetadata.class);

        FlatMapOperator<BandMetadata, Patch> patches_data = new FlatMapOperator<>(
                t -> createPatchesPerBand(t),
                BandMetadata.class,
                Patch.class
        );


//        LocalCallbackSink<BandMetadata> sink = LocalCallbackSink.createCollectingSink(result, BandMetadata.class);

        String outputFileUrl = "file:///Users/rodrigopardomeza/tu-berlin/agoraeo/agoraeo/outputs/patches.log";
        TextFileSink<Patch> sink = new TextFileSink<>(outputFileUrl, Patch.class);

        // his band.resolution = our pixel_resolution

        source.connectTo(0, l2a_catalog,0);
        l2a_catalog.connectTo(0, bands,0);
        bands.connectTo(0, metadata,0);
        metadata.connectTo(0, patches_data,0);
        patches_data.connectTo(0, sink,0);
//        source.connectTo(0,toL2A,0);
//        toL2A.connectTo(0,sink,0);


        return new WayangPlan(sink);
    }

    private static List<Patch> createPatchesPerBand(BandMetadata t) {
        /* TODO: Pipeline to get the data for patches from each Band */
        Tuple2<Integer, Integer> steps = t.getSteps();
        List<Patch> patches = new ArrayList<>();

        for(int x = 0; x < steps.field0; x++) {
            for(int y = 0; y < steps.field1; y++) {
//                System.out.println("Creating Patch: " + t.getUtm() + "|" + t.getBand_name() + "|" + x + "," + y);

                Tuple4<Double, Double, Double, Double> patchCoords = getPatchCoords(x, y, t.getUl(), t.getPatch_size(), t.getPixel_resolution());

                /* Patch size is the number of pixels of the patch
                   xoff if the number of pixels to skip before reading in x dimension */
                int xoff = x * t.getPatch_size().field0;
                int yoff = y * t.getPatch_size().field1;

                /* While the patch_size is the number of pixels to read */
                // TODO: Impossible to create Numeric Type Numpy Array in Java. Can we generate it in Python as output?
                ByteBuffer buffy = t.getBand_raster().ReadRaster_Direct(xoff, yoff, t.getPatch_size().field0, t.getPatch_size().field1);
                byte[] data = new byte[buffy.remaining()];
                buffy.get(data);

                Patch patch = new Patch(
                        t.getUtm(),
                        t.getBand_resolution(),
                        t.getBand_name(),
                        patchCoords._1(),
                        patchCoords._2(),
                        patchCoords._3(),
                        patchCoords._4(),
                        new Tuple2<>(x, y),
                        t.getEspg(),
                        t.getProjection(),
                        data,
                        t.getPixel_resolution()
                );

                patches.add(patch);
            }
        }

        return patches;
    }

    /* Returns Specific coordinates of the Patch in this specific Band regarding its resolution
        (Not necessarily the coordinates in the Raster(s), Model only provide geographic space integrity) */
    private static Tuple4<Double, Double, Double, Double> getPatchCoords(int x, int y, Tuple2<Double, Double> ul, Tuple2<Integer, Integer> patch_size, Tuple2<Double, Double> pixel_resolution) {

        /* Patch size (in pixels) is defined regarding the pixel resolution, coord_x_step multiplies it by
            pixel_resolution (long in mm) translating it into coordinates*/
        double coord_x_step = patch_size.field0 * pixel_resolution.field0;
        double coord_y_step = patch_size.field1 * pixel_resolution.field1;

        /* Calculate the limits of the corners for current patch */
        Double ulx = ul.field0 + x * coord_x_step;
        Double uly = ul.field1 + y * coord_y_step;
        Double lrx = ul.field0 + (x + 1) * coord_x_step;
        Double lry = ul.field1 + (y + 1) * coord_y_step;
        return new Tuple4<>(ulx, uly, lrx, lry);
    }

    private static List<Band> processResolution(String img_folder, String resolution, L2aFile l2A_file) {

        // Missing to filter only the bands relevant to each resolution
        return Stream.of(Objects.requireNonNull(new File(img_folder + "/" + resolution).listFiles()))
                .filter(file -> file.isFile() &&
                        (Objects.equals(file.getName().substring(file.getName().lastIndexOf(".")+1), "jp2")))
                .map(t -> {
                    String[] parts = t.getName().split("_");
                    return new Band(
                            l2A_file.getL1c_uuid(),
                            l2A_file.getL2a_uuid(),
                            l2A_file.getL2a_path(),
                            parts[2],
                            img_folder + "/" + resolution + "/" + t.getName(),
                            parts[3].substring(0,2),
                            parts[0]
                    );

                })
                .filter(t -> !Arrays.asList("TCI", "AOT", "WVP").contains(t.getBand_name()))
                .filter(t -> consistentResolutionMapping(t.getResolution(), t.getBand_name()))
                .collect(Collectors.toList());
    }

    private static boolean consistentResolutionMapping(String resolution, String band_name) {

        return Objects.equals(resolutionMap.get(band_name), resolution);
    }

}