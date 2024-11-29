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

 package org.apache.wayang.apps.wordcount;

 import org.apache.wayang.basic.data.Tuple2;
 import org.apache.wayang.core.api.Configuration;
 import org.apache.wayang.core.api.WayangContext;
 import org.apache.wayang.core.plan.wayangplan.WayangPlan;
 import org.apache.wayang.core.util.ReflectionUtils;
 import org.apache.wayang.java.Java;
 import org.apache.wayang.java.platform.JavaPlatform;
 import org.apache.wayang.spark.Spark;
 import org.apache.wayang.api.JavaPlanBuilder;
 
 
 import java.io.IOException;
 import java.net.URISyntaxException;
 import java.util.Arrays;
 import java.util.Collection;
 import java.util.LinkedList;
 import java.util.List;
 import java.util.OptionalDouble;
 
 public class Test1 {
 
     public static void main(String[] args) throws IOException, URISyntaxException {
         try {
 
 
             System.out.println("I AM RUNNING MAIN CLASS IN MAIN FUNCTION!!..........:DXDi12u312ij3asdpijasjhdasghejh!");
 
 
             if (args.length == 0) {
                 System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
                 System.exit(1);
             }
 
             WayangContext wayangContext = new WayangContext();
             for (String platform : args[0].split(",")) {
                 switch (platform) {
                     case "java":
                         wayangContext.register(Java.basicPlugin());
                         break;
                     case "spark":
                         wayangContext.register(Spark.basicPlugin());
                         break;
                     default:
                         System.err.format("Unknown platform: \"%s\"\n", platform);
                         System.exit(3);
                         return;
                 }
             }
            
             /* Get a plan builder */
             JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                     .withJobName("WordCount")
                     .withUdfJarOf(Main.class);
                     
             
            for(int i = 0; i<5; i++){
                if(args[1].equals("AS3")){ 
                    long startTime = System.nanoTime();
                    /* Start building the Apache WayangPlan */
                    Collection<Tuple2<String, Integer>>wordcounts = planBuilder
                    /* Read the text file */
                    .readAmazonS3File(args[2], args[3], args[4]).withName("Load file")
                    /* Split each line by non-word characters */
                    .flatMap(line -> Arrays.asList(line.split("\\W+")))
                    .withSelectivity(1, 100, 0.9)
                    .withName("Split words")
    
                    /* Filter empty tokens */
                    .filter(token -> !token.isEmpty())
                    .withName("Filter empty words")
    
                    /* Attach counter to each word */
                    .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
    
                    // Sum up counters for every word.
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    )
                    .withName("Add counters")
    
                    /* Execute the plan and collect the results */
                    .collect();
    
                    long endTime = System.nanoTime();
                    long duration = (endTime - startTime) / 1_000_000;
                    System.out.println("running time:" + duration);
                    System.out.printf("Found %d words:\n", wordcounts.size());
                }
    
                else{
                    long startTime = System.nanoTime();
                    /* Start building the Apache WayangPlan */
                    Collection<Tuple2<String, Integer>>wordcounts = planBuilder
                    /* Read the text file */
                    .readTextFile(args[1]).withName("Load file")
                    /* Split each line by non-word characters */
                    .flatMap(line -> Arrays.asList(line.split("\\W+")))
                    .withSelectivity(1, 100, 0.9)
                    .withName("Split words")
    
                    /* Filter empty tokens */
                    .filter(token -> !token.isEmpty())
                    .withName("Filter empty words")
    
                    /* Attach counter to each word */
                    .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")
    
                    // Sum up counters for every word.
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    )
                    .withName("Add counters")
    
                    /* Execute the plan and collect the results */
                    .collect();
    
                    long endTime = System.nanoTime();
                    long duration = (endTime - startTime) / 1_000_000;
                    System.out.println("running time:" + duration);
                    System.out.printf("Found %d words:\n", wordcounts.size());
                }
            }
 
             //wordcounts.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
         } catch (Exception e) {
             System.err.println("App failed.");
             e.printStackTrace();
             System.exit(4);
         }
     }
 }
 
 