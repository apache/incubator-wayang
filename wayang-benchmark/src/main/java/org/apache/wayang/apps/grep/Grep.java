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

package org.apache.wayang.apps.grep;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;

public class Grep implements Serializable {

  public static void pureJava(String input, String output) throws IOException {
    Iterator<CharSequence> out = Files.lines(Paths.get(input))
        .filter(line -> line.contains("six"))
        .map(str -> (CharSequence) str)
        .iterator();

    Files.write(
        Paths.get(output),
        new Iterable<CharSequence>() {
          @Override
          public Iterator<CharSequence> iterator() {
            return out;
          }
        }
    );
  }

  public static void pureSpark(String input, String output) throws IOException {
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("grep");
    SparkContext context = new SparkContext(conf);

    context
        .textFile(input, context.defaultParallelism())
        .toJavaRDD()
        .filter( line -> line.contains("six"))
        .saveAsTextFile(output);
  }

  public static void pureFlink(String input, String output) throws Exception {
    // Create an environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    env.readTextFile(input)
        .filter(line -> line.contains("six"))
        .writeAsText(output);

    env.execute();
  }

  public static void wayangPlatform(String input, String output, Plugin plugin){
    // Set up WayangContext.
    WayangContext wayangContext = new WayangContext().with(plugin);

    // Build and execute a Wayang plan.
    new JavaPlanBuilder(wayangContext)
        .readTextFile(input)
        .filter(line -> line.contains("six")).withName("Split words")
        .writeTextFile(output, s -> s, "lala");
  }

  public static void wayangJava(String input, String output){
    wayangPlatform(input, output, Java.basicPlugin());
  }

  public static void wayangSpark(String input, String output){
    wayangPlatform(input, output, Spark.basicPlugin());
  }

  public static void wayangFlink(String input, String output){
    wayangPlatform(input, output, Flink.basicPlugin());
  }

  public static void main(String... args) throws Exception {
    int size = Integer.parseInt(args[0]);
    String platform = args[1];

    String input = args[2]+"/python/src/pywy/tests/resources/10e"+size+"MB.input";
    String output = args[2]+"/lala.out";

    String[] command = {"rm", "-r", output};
    Process process = Runtime.getRuntime().exec(command);

    long pre = System.currentTimeMillis();
    switch (platform){
      case "so":
        Runtime.getRuntime().exec(
            String.format(
                "grep \"six\" %s > %s",
                input,
                output
            )
        );
        break;
      case "pure-java":
        Grep.pureJava(input, output);
        break;
      case "pure-spark":
        Grep.pureSpark("file://"+input, "file://"+output);
        break;
      case "pure-flink":
        Grep.pureFlink(input, output);
        break;
      case "wayang-java":
        Grep.wayangJava("file://"+input, "file://"+output);
        break;
      case "wayang-spark":
        Grep.wayangSpark("file://"+input, "file://"+output);
        break;
      case "wayang-flink":
        Grep.wayangFlink("file://"+input, output);
        break;
    }
    long after = System.currentTimeMillis();
    System.out.println(
      String.format(
        "the platform %s took %f s",
        platform,
        (float)((after - pre)/1000.0)
      )
    );

  }
}
