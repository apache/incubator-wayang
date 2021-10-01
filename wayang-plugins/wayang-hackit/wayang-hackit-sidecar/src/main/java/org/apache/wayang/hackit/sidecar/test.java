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
package org.apache.wayang.hackit.sidecar;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.reflections.Reflections;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.jar.JarOutputStream;
import java.util.jar.JarEntry;
import java.util.zip.ZipEntry;

public class test {

    public static void main(String... args) throws Exception {
        System.out.println(Arrays.toString(getJars()));
        ExecutionEnvironment env = ExecutionEnvironment
                .createRemoteEnvironment(
                        "127.0.0.1",
                        8081,
                        //"/Users/rodrigopardomeza/tu-berlin/debugger/wayang-plugins/wayang-hackit/wayang-hackit-sidecar/target/wayang-hackit-sidecar-0.6.0-SNAPSHOT.jar"
                        getJars()
                );

        String clusterPath = "/mnt/example/";
        //DataSet<String> data = env.readTextFile("/Users/rodrigopardomeza/flink/count");
        //DataSet<String> data = env.readTextFile(clusterPath + "count");
        DataSet<String> data = env.readTextFile("/mnt/example/count");

        System.out.println(data.count());

        DataSet<String> words = data.flatMap(
                /*(FlatMapFunction<String, String>) (s,l) -> {
                    for (String ss : s.split(" ")) {
                        l.collect(ss);
                    }
                }*/
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] wor = s.split(" ");
                        for (String ss: wor
                        ) {
                            collector.collect(ss);
                        }
                    }
                }

        );
        Function<Integer, Integer> lala = a -> a + 2;
        DataSet<String> data2 = words
                .filter((FilterFunction<String>) value -> value.startsWith("T"));
        /*DataSet<String> words = data.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] wor = s.split(" ");
                        for (String ss: wor
                             ) {
                            collector.collect(ss);
                        }
                    }
                }

        );*/
                        //.filter(line -> line.startsWith("T"))
        data2.writeAsText( "/mnt/example/cluster_4", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
                //.writeAsText("/Users/rodrigopardomeza/flink/cluster_babe");

        env.execute();

    }

    private static String[] getJars(){
        List<String> jars = new ArrayList<>(5);
        List<Class> clazzs = Arrays.asList(new Class[]{test.class});

        clazzs.stream().map(
                test::getDeclaringJar
        ).filter(
                element -> element != null
        ).forEach(jars::add);

        return jars.toArray(new String[0]);
    }

    public static String getDeclaringJar(Class<?> cls) {
        try {
            final URL location = cls.getProtectionDomain().getCodeSource().getLocation();
            final URI uri = location.toURI();
            String path = uri.getPath();
            if (path.endsWith(".jar")) {
                return path;
            } else {
                System.out.println(
                    String.format(
                        "Class %s is not loaded from a JAR file, but from %s. Thus, cannot provide the JAR file.",
                            cls,
                            path
                    )
                );
                path = createJAR(cls);
                return path;
            }
        } catch (Exception e) {
            System.out.println(
                String.format(
                        "Could not determine JAR file declaring %s.",
                        cls
                )
            );
        }
        return null;
    }

    public static String createJAR(Class clazz){
        try {
            Path tmpCustomPrefix = Files.createTempDirectory("lala");
            String path_final = tmpCustomPrefix.toString() + "/lala.jar";
            //String path_final = "/Users/rodrigopardomeza/tu-berlin/debugger/lala.jar";
            System.out.println(path_final);

            FileOutputStream fout = new FileOutputStream(path_final);
            JarOutputStream jarOut = new JarOutputStream(fout);


            Class[] clazzs = getClasses(clazz.getPackage().getName());

            for(int i = 0; i < clazzs.length; i++){
                addClass(clazzs[i], jarOut);
            }



            jarOut.close();
            fout.close();
            return path_final;

        } catch (Exception e){

        }
        return null;
    }

    private static void addClass(Class c, JarOutputStream jarOutputStream) throws IOException {
        String path = c.getName().replace('.', '/') + ".class";
        jarOutputStream.putNextEntry(new JarEntry(path));
        jarOutputStream.write(toByteArray(c.getClassLoader().getResourceAsStream(path)));
        jarOutputStream.closeEntry();
    }

    public static byte[] toByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[0x1000];
        while (true) {
            int r = in.read(buf);
            if (r == -1) {
                break;
            }
            out.write(buf, 0, r);
        }
        return out.toByteArray();
    }

    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param packageName The base package
     * @return The classes
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private static Class[] getClasses(String packageName)
            throws ClassNotFoundException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assert classLoader != null;
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(path);
        List<File> dirs = new ArrayList<File>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }
        ArrayList<Class> classes = new ArrayList<Class>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes.toArray(new Class[classes.size()]);
    }

    /**
     * Recursive method used to find all classes in a given directory and subdirs.
     *
     * @param directory   The base directory
     * @param packageName The package name for classes found inside the base directory
     * @return The classes
     * @throws ClassNotFoundException
     */
    private static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class> classes = new ArrayList<Class>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        for (File file : files) {
            //if (file.isDirectory()) {
            //    assert !file.getName().contains(".");
            //    classes.addAll(findClasses(file, packageName + "." + file.getName()));
            //} else
            if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }
}

