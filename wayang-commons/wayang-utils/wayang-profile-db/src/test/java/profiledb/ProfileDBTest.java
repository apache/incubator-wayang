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

package profiledb;

import org.junit.Assert;
import org.junit.Test;
import profiledb.measurement.TestMemoryMeasurement;
import profiledb.measurement.TestTimeMeasurement;
import profiledb.model.Experiment;
import profiledb.model.Measurement;
import profiledb.model.Subject;
import profiledb.storage.FileStorage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ProfileDBTest {

    @Test
    public void testPolymorphSaveAndLoad() throws IOException {

        try {
            Path temp = Files.createTempFile("", ".tmp");

            String absolutePath = temp.toString();
            System.out.println("Temp file : " + absolutePath);

            URI uri = new URI("my-file4");

            FileStorage store = new FileStorage(uri);

            ProfileDB profileDB = new ProfileDB(store)
                    .registerMeasurementClass(TestMemoryMeasurement.class)
                    .registerMeasurementClass(TestTimeMeasurement.class);

            final Experiment experiment = new Experiment("test-xp", new Subject("PageRank", "1.0"), "test experiment");

            Measurement timeMeasurement = new TestTimeMeasurement("exec-time", 12345L);
            Measurement memoryMeasurement = new TestMemoryMeasurement("exec-time", System.currentTimeMillis(), 54321L);

            experiment.addMeasurement(timeMeasurement);
            experiment.addMeasurement(memoryMeasurement);

            // Save the experiment.
            byte[] buffer;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            profileDB.save(Collections.singleton(experiment), bos);
            bos.close();
            buffer = bos.toByteArray();
            System.out.println("Buffer contents: " + new String(buffer, "UTF-8"));

            // Load the experiment.
            ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
            Collection<Experiment> loadedExperiments = profileDB.load(bis);

            // Compare the experiments.
            Assert.assertEquals(1, loadedExperiments.size());
            Experiment loadedExperiment = loadedExperiments.iterator().next();
            Assert.assertEquals(experiment, loadedExperiment);

            // Compare the measurements.
            Assert.assertEquals(2, loadedExperiment.getMeasurements().size());
            Set<Measurement> expectedMeasurements = new HashSet<>(2);
            expectedMeasurements.add(timeMeasurement);
            expectedMeasurements.add(memoryMeasurement);
            Set<Measurement> loadedMeasurements = new HashSet<>(loadedExperiment.getMeasurements());
            Assert.assertEquals(expectedMeasurements, loadedMeasurements);

        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRecursiveSaveAndLoad() throws IOException {
        try {
            URI uri = new URI("my-file2");
            FileStorage store = new FileStorage(uri);

            ProfileDB profileDB = new ProfileDB(store)
                    .registerMeasurementClass(TestMemoryMeasurement.class)
                    .registerMeasurementClass(TestTimeMeasurement.class);

            // Create an example experiment.
            final Experiment experiment = new Experiment("test-xp", new Subject("PageRank", "1.0"), "test experiment");
            TestTimeMeasurement topLevelMeasurement = new TestTimeMeasurement("exec-time", 12345L);
            TestTimeMeasurement childMeasurement = new TestTimeMeasurement("sub-exec-time", 2345L);
            topLevelMeasurement.addSubmeasurements(childMeasurement);
            experiment.addMeasurement(topLevelMeasurement);

            // Save the experiment.
            byte[] buffer;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            profileDB.save(Collections.singleton(experiment), bos);
            bos.close();
            buffer = bos.toByteArray();
            System.out.println("Buffer contents: " + new String(buffer, "UTF-8"));

            // Load the experiment.
            ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
            Collection<Experiment> loadedExperiments = profileDB.load(bis);

            // Compare the experiments.
            Assert.assertEquals(1, loadedExperiments.size());
            Experiment loadedExperiment = loadedExperiments.iterator().next();
            Assert.assertEquals(experiment, loadedExperiment);

            // Compare the measurements.
            Assert.assertEquals(1, loadedExperiment.getMeasurements().size());
            final Measurement loadedMeasurement = loadedExperiment.getMeasurements().iterator().next();
            Assert.assertEquals(topLevelMeasurement, loadedMeasurement);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFileOperations() throws IOException {
        File tempDir = Files.createTempDirectory("profiledb").toFile();
        File file = new File(tempDir, "profiledb.json");
        file.createNewFile();
        FileStorage store = new FileStorage(file.toURI());

        ProfileDB profileDB = new ProfileDB(store)
                .registerMeasurementClass(TestMemoryMeasurement.class)
                .registerMeasurementClass(TestTimeMeasurement.class);

        // Create example experiments.
        final Experiment experiment1 = new Experiment("xp1", new Subject("PageRank", "1.0"), "test experiment 1");
        experiment1.addMeasurement(new TestTimeMeasurement("exec-time", 1L));
        final Experiment experiment2 = new Experiment("xp2", new Subject("KMeans", "1.1"), "test experiment 2");
        experiment2.addMeasurement(new TestTimeMeasurement("exec-time", 2L));
        final Experiment experiment3 = new Experiment("xp3", new Subject("Apriori", "2.0"), "test experiment 3");
        experiment3.addMeasurement(new TestMemoryMeasurement("ram", System.currentTimeMillis(), 3L));

        // Save the experiments.
        profileDB.save(experiment1);
        profileDB.append(experiment2, experiment3);

        Files.lines(file.toPath()).forEach(System.out::println);

        // Load and compare.
        final Set<Experiment> loadedExperiments = new HashSet<>(profileDB.load());
        final List<Experiment> expectedExperiments = Arrays.asList(experiment1, experiment2, experiment3);
        Assert.assertEquals(expectedExperiments.size(), loadedExperiments.size());
        Assert.assertEquals(new HashSet<>(expectedExperiments), new HashSet<>(loadedExperiments));
    }

    @Test
    public void testAppendOnNonExistentFile() throws IOException {

        File tempDir = Files.createTempDirectory("profiledb").toFile();
        File file = new File(tempDir, "new-profiledb.json");
        file.createNewFile();
        FileStorage store = new FileStorage(file.toURI());

        // This seems to be an issue on Linux.
        ProfileDB profileDB = new ProfileDB(store)
                .registerMeasurementClass(TestMemoryMeasurement.class)
                .registerMeasurementClass(TestTimeMeasurement.class);

        // Create example experiments.
        final Experiment experiment1 = new Experiment("xp1", new Subject("PageRank", "1.0"), "test experiment 1");
        experiment1.addMeasurement(new TestTimeMeasurement("exec-time", 1L));

        // Save the experiments.
        Assert.assertTrue(!file.exists() || file.delete());
        profileDB.append(experiment1);

        Files.lines(file.toPath()).forEach(System.out::println);

        // Load and compare.
        final Set<Experiment> loadedExperiments = new HashSet<>(profileDB.load());
        final List<Experiment> expectedExperiments = Collections.singletonList(experiment1);
        Assert.assertEquals(expectedExperiments.size(), loadedExperiments.size());
        Assert.assertEquals(new HashSet<>(expectedExperiments), new HashSet<>(loadedExperiments));
    }

}
