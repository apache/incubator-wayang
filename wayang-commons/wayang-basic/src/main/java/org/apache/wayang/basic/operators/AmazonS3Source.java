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

import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;


/**
 * This source reads a blob file stored in Amazon s3 and outputs the lines as data units.
 */

public class AmazonS3Source extends UnarySource<String> {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final String encoding;
    private final String bucket;
    private final String blobName;
    private final String filePathToCredentialsFile;

    public AmazonS3Source(String bucket, String blobName, String filePathToCredentialsFile) {
        this(bucket, blobName, filePathToCredentialsFile, "UTF-8");
    }
    

    public AmazonS3Source(String bucket, String blobName, String filePathToCredentialsFile, String encoding) {
        super(DataSetType.createDefault(String.class));
        this.encoding = encoding;
        this.filePathToCredentialsFile = filePathToCredentialsFile;
        this.bucket = bucket;
        this.blobName = blobName;

    }

     /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */

    public AmazonS3Source(AmazonS3Source that) {
        super(that);
        this.encoding = that.getEncoding();
        this.bucket = that.getBucket();
        this.blobName = that.getBlobName();
        this.filePathToCredentialsFile = that.getFilePathToCredentialsFile();

    }    

    public String getEncoding() {
        return encoding;
    }

    public String getFilePathToCredentialsFile() {
        return filePathToCredentialsFile;
    }


    public String getBucket() {
        return bucket;
    }

    public String getBlobName() {
        return blobName;
    }

    /**
     * 
     * @return the total size of the bytes in the Blob file.
     */
    public OptionalLong getBlobByteSize() {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
            .bucket(getBucket())
            .key(getBlobName())
            .build();

        HeadObjectResponse headObjectResponse = AmazonS3SourceHelpers.getS3Client(filePathToCredentialsFile).headObject(headObjectRequest);
        return OptionalLong.of(headObjectResponse.contentLength()); // returns the size in bytes
        } 
        catch (Exception ex) {
            AmazonS3Source.this.logger.warn("Failed to esimate bytes per line with error: " + ex, AmazonS3Source.this.blobName);
            ex.printStackTrace();
            return OptionalLong.empty();
        }  
    }

    
    /**
     * Retrieves an InputStream to the specified S3 blob file.
     * 
     * @return InputStream to the Blob file.
     * @throws Exception if an error occurs during S3 client creation or file rertrieval.
     */
    public InputStream getInputStream() throws Exception {
        return AmazonS3SourceHelpers.getS3Client(filePathToCredentialsFile).getObject(AmazonS3SourceHelpers.getGetObjectRequest(bucket, blobName));
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
            Validate.isTrue(AmazonS3Source.this.getNumInputs() == inputEstimates.length);

            // see Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
            );
            
            // Query the job cache first to see if there is already an estimate.
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), AmazonS3Source.this.blobName);
            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);

            if (cardinalityEstimate != null) return  cardinalityEstimate;

            // Otherwise calculate the cardinality.
            // First, inspect the size of the file and its line sizes.
            OptionalLong fileSize = getBlobByteSize();


            if (!fileSize.isPresent()) {
                AmazonS3Source.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                        AmazonS3Source.this.blobName);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;

            } else if (fileSize.getAsLong() == 0L) {
                timeMeasurement.stop();
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            OptionalDouble bytesPerLine = this.estimateBytesPerLine();

            if (!bytesPerLine.isPresent()) {
                AmazonS3Source.this.logger.warn("Could not determine average line size of {}... deliver fallback estimate.",
                        AmazonS3Source.this.blobName);
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
            ResponseInputStream<GetObjectResponse> responseInputStream = AmazonS3SourceHelpers.getS3Client(filePathToCredentialsFile).getObject(AmazonS3SourceHelpers.getGetObjectRequest(bucket, blobName));    
        
            final int KiB = 1024;
            final int MiB = KiB * 1024; // 1 MiB
    
            try (LimitedInputStream lis = new LimitedInputStream(responseInputStream, 1 * MiB)) {
                final BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(lis, AmazonS3Source.this.encoding)
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
                    AmazonS3Source.this.logger.warn("Could not find any newline character in {}.", AmazonS3Source.this.blobName);
                    return OptionalDouble.empty();
                }
    
                return OptionalDouble.of((double) lis.getNumReadBytes() / numLineFeeds);
            }
        }

       
        catch (Exception e) {
            AmazonS3Source.this.logger.error("Could not estimate bytes per line of an input file.", e);
        }

        return OptionalDouble.empty();
        }
    }

    /**
     * A static helper class containing utility methods for the {@link AmazonS3Source} class.
     */

    private static class AmazonS3SourceHelpers {
        private static S3Client getS3Client(String filePathToCredentialsFile) throws IOException, JSONException{

            JSONObject credentialsJson = getJsonObjectFromFile(filePathToCredentialsFile);
            String accessKey = getObjectFromJson(credentialsJson, "accessKey"); 
            String secretKey = getObjectFromJson(credentialsJson, "secretAccessKey"); 
            Region region = Region.of(getObjectFromJson(credentialsJson, "region")); 
            
            StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create( AwsBasicCredentials.create(accessKey, secretKey) );
            
            return S3Client.builder()
                    .region(region)
                    .credentialsProvider(credentialsProvider)
                    .build();
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

        /**
         * @param bucketName the bucket to connect to
         * @param blobName the blob to connect to. Should reside in the @param bucketName
         * @return an Object Request for the given Bucket and Blob file
         */
        private static GetObjectRequest getGetObjectRequest(String bucketName, String blobName ) {
            return GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobName)
                .build();
        }
    }
    
}
