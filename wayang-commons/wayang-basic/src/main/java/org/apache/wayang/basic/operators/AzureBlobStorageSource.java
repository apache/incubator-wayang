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


package org.apache.wayang.basic.operators;


import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.commons.lang3.Validate;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.LimitedInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import java.nio.file.Files;
import java.io.InputStream;

import org.json.JSONException;
import org.json.JSONObject;

import com.azure.storage.blob.*;


/**
 * This source reads a blob file stored in Azure Blob Storage and outputs the lines as data units.
 */

public class AzureBlobStorageSource extends UnarySource<String> {

    private final Logger logger = LogManager.getLogger(this.getClass());
    private final String encoding;
    private final String storageContainer;
    private final String blobName;
    private final String filePathToCredentialsFile;

    public AzureBlobStorageSource(String storageContainer, String blobName, String filePathToCredentialsFile) {
        this(storageContainer, blobName, filePathToCredentialsFile, "UTF-8");
    }

    public AzureBlobStorageSource(String storageContainer, String blobName, String filePathToCredentialsFile, String encoding) {
        super(DataSetType.createDefault(String.class));
        this.encoding = encoding;
        this.filePathToCredentialsFile = filePathToCredentialsFile;
        this.storageContainer = storageContainer;
        this.blobName = blobName;

    }


     /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */

    public AzureBlobStorageSource(AzureBlobStorageSource that) {
        super(that);
        this.encoding = that.getEncoding();
        this.storageContainer = that.getStorageContainer();
        this.blobName = that.getBlobName();
        this.filePathToCredentialsFile = that.getFilePathToCredentialsFile();

    }

    public String getEncoding() {
        return this.encoding;
    }

    public String getStorageContainer() {
        return this.storageContainer;
    }

    public String getBlobName() {
        return this.blobName;
    }

    public String getFilePathToCredentialsFile() {
        return this.filePathToCredentialsFile;
    }

    /**
     * 
     * @return the total size of the bytes in the Blob file. Returns empty {@link OptionalLong} if an exception is caught.
     */
    public OptionalLong getBlobByteSize() {
        try {
            BlobClient blobClient = AzureBlobStorageSourceHelpers.getBlobClient(
                filePathToCredentialsFile, 
                storageContainer, 
                blobName);

        return OptionalLong.of(blobClient.getProperties().getBlobSize()); // returns the size in bytes
        } 
        catch (Exception ex) {
            AzureBlobStorageSource.this.logger.warn("Failed to esimate bytes per line with error: " + ex, AzureBlobStorageSource.this.blobName);
            ex.printStackTrace();
            return OptionalLong.empty();
        }  
    }

    /**
     * Retrieves an InputStream to the specified Azure blob file.
     * 
     * @return InputStream to the Blob file.
     * @throws Exception if an error occurs during Azure client creation or file rertrieval.
     */
    public InputStream getInputStream() throws Exception {

        BlobClient blobClient = AzureBlobStorageSourceHelpers.getBlobClient(
            filePathToCredentialsFile, 
            storageContainer, 
            blobName);
        return blobClient.openInputStream();
    }

     /**
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    protected class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

        public static final double CORRECTNESS_PROBABILITY = 0.95d;

        /**
         * We expect selectivities to be correct within a factor of {@value #EXPECTED_ESTIMATE_DEVIATION}.
         */
        public static final double EXPECTED_ESTIMATE_DEVIATION = 0.05;

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(AzureBlobStorageSource.this.getNumInputs() == inputEstimates.length);

            // see Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
            );
            
            // Query the job cache first to see if there is already an estimate.
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), AzureBlobStorageSource.this.blobName);
            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);

            if (cardinalityEstimate != null) return  cardinalityEstimate;

            // Otherwise calculate the cardinality.
            // First, inspect the size of the file and its line sizes.
            OptionalLong fileSize = getBlobByteSize();


            if (!fileSize.isPresent()) {
                AzureBlobStorageSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                    AzureBlobStorageSource.this.blobName);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;

            } else if (fileSize.getAsLong() == 0L) {
                timeMeasurement.stop();
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            OptionalDouble bytesPerLine = this.estimateBytesPerLine();

            if (!bytesPerLine.isPresent()) {
                AzureBlobStorageSource.this.logger.warn("Could not determine average line size of {}... deliver fallback estimate.",
                    AzureBlobStorageSource.this.blobName);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }

            // Extrapolate a cardinality estimate for the complete file.
            double numEstimatedLines = fileSize.getAsLong() / bytesPerLine.getAsDouble();
            double expectedDeviation = numEstimatedLines * EXPECTED_ESTIMATE_DEVIATION;
            cardinalityEstimate = new CardinalityEstimate(
                    (long) (numEstimatedLines - expectedDeviation),
                    (long) (numEstimatedLines + expectedDeviation),
                    CORRECTNESS_PROBABILITY
            );

            // Cache the result, so that it will not be recalculated again.
            optimizationContext.putIntoJobCache(jobCacheKey, cardinalityEstimate);

            timeMeasurement.stop();
            return cardinalityEstimate;
        }


    /**
     * Estimate the number of bytes that are in each line of a given file.
     *
     * @return the average number of bytes per line if it could be determined
     */
    private OptionalDouble estimateBytesPerLine() {

        try {

            final int KiB = 1024;
            final int MiB = KiB * 1024; // 1 MiB
    
            try (LimitedInputStream lis = new LimitedInputStream(AzureBlobStorageSource.this.getInputStream(), 1 * MiB)) {
                final BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(lis, AzureBlobStorageSource.this.encoding)
                );
    
                // Read as much as possible.
                char[] cbuf = new char[1024];
                int numReadChars, numLineFeeds = 0;
                while ((numReadChars = bufferedReader.read(cbuf)) != -1) {
                    
                    for (int i = 0; i < numReadChars; i++) {
                        if (cbuf[i] == '\n') {
                            numLineFeeds++;
                        }
                    }
                }
    
                if (numLineFeeds == 0) {
                    AzureBlobStorageSource.this.logger.warn("Could not find any newline character in {}.", AzureBlobStorageSource.this.blobName);
                    return OptionalDouble.empty();
                }
    
                return OptionalDouble.of((double) lis.getNumReadBytes() / numLineFeeds);
            }
        }

       
        catch (Exception e) {
            AzureBlobStorageSource.this.logger.error("Could not estimate bytes per line of an input file.", e);
        }

        return OptionalDouble.empty();
        }
    }
    

    /**
     * A static helper class containing utility methods for the {@link AzureBlobStorageSource} class.
     */
    private static class AzureBlobStorageSourceHelpers {

        /**
         * Creates a {@link BlobClient} for a specified blob in an Azure Blob Storage container.
         *
         * @param filePathToCredentialsFile the path to the file containing credentials.
         * @param containerName the name of the container.
         * @param blobName the name of the blob file in the container.
         * @return the {@link BlobClient} for the specified blob.
         * @throws JSONException if an error occurs while parsing the credentials JSON.
         * @throws IOException if an I/O error occurs while reading the credentials file.
         * @throws Exception if an error occurs during the creation of the client.
         */
        public static BlobClient getBlobClient(String filePathToCredentialsFile, String containerName, String blobName) 
                throws JSONException, IOException, Exception {
            return getBlobClient(
                getBlobContainerClient(filePathToCredentialsFile, containerName), 
                blobName);
        }

        /**
         * Creates a {@link BlobClient} for a specified blob using a given {@link BlobContainerClient}.
         *
         * @param blobContainerClient the {@link BlobContainerClient} representing the container.
         * @param blobFile the name of the blob file to create the client for.
         * @return the {@link BlobClient} for the specified blob.
         */
        public static BlobClient getBlobClient(BlobContainerClient blobContainerClient, String blobFile) {
            return blobContainerClient.getBlobClient(blobFile);
        }

        /**
         * Creates a {@link BlobContainerClient} for the specified container.
         *
         * @param filePathToCredentialsFile the path to the file containing credentials.
         * @param containerName the name of the container.
         * @return the {@link BlobContainerClient} for the specified container.
         * @throws JSONException if an error occurs while parsing the credentials JSON.
         * @throws IOException if an I/O error occurs while reading the credentials file.
         * @throws Exception if an error occurs during the creation of the container client.
         */
        public static BlobContainerClient getBlobContainerClient(String filePathToCredentialsFile, String containerName) 
                throws JSONException, IOException, Exception {
            String connectionString = getObjectFromJson(getJsonObjectFromFile(filePathToCredentialsFile), "connectionString");
            return getBlobServiceClient(connectionString).getBlobContainerClient(containerName);
        }

        /**
         * Creates a {@link BlobServiceClient} using a connection string.
         *
         * @param connectionString the Azure Blob Storage connection string.
         * @return the {@link BlobServiceClient} created using the provided connection string.
         * @throws IllegalArgumentException if the connection string is invalid.
         * @throws NullPointerException if the connection string is null.
         */
        private static BlobServiceClient getBlobServiceClient(String connectionString) 
                throws IllegalArgumentException, NullPointerException {
            return new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .buildClient();
        }


        /**
         * Reads and gets a JSON object from a file
         * 
         * @param file the file to read the JSON object from
         * @return the JSONObject parsed from the file's content
         * @throws IOException if an I/O error occurs while reading the file
         */
        private static JSONObject getJsonObjectFromFile(String file) throws IOException {
            return new JSONObject(
                 new String(
                     Files.readAllBytes(
                         Paths.get(file)))
             ); 
         }

        /**
         * Fetches the value associated with the given key from a JSON object.
         * @param credentialsJson the JSONObject from which the value should be retrieved.
         * @param key the key to search for in the JSON object.
         * @return the value associated with the provided key in the JSON object.
         * @throws JSONException if the key is not found or the retrieval fails.
         */
        private static String getObjectFromJson(JSONObject credentialsJson, String key) throws JSONException {
            try {
                return credentialsJson.getString(key);
            } catch (JSONException jsonException) {
                throw jsonException;
            }
        }

    }
    
}
