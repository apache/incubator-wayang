package org.apache.wayang.agoraeo.operators.spark;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Iterator;

public class Pepito implements Serializable, FlatMapFunction<String, String> {

    private final String sen2cor;
    private final String l2a_location;

    public Pepito(String sen2cor, String l2a_location) {

        this.sen2cor = sen2cor;
        this.l2a_location = l2a_location;
    }

    @Override
    public Iterator<String> call(String s) throws Exception {
        try {
            String command = sen2cor + " " +
                    s + " " +
                    " --output_dir " + l2a_location;
            Process process = Runtime.getRuntime().exec(command);
            Iterator<String> input = new BufferedReader(
                    new InputStreamReader(
                            process.getInputStream()
                    )
            ).lines().iterator();
            return input;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
