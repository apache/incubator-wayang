package org.apache.wayang.basic.operators;


import com.google.cloud.storage.Bucket;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;


import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.LimitedInputStream;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.nio.channels.Channels;
public class GoogleCloudStorageSource extends UnarySource<String> {

    private final Logger logger = LogManager.getLogger(this.getClass());

    //TODO is this needed?
    private final String inputUrl;

    private final String encoding;
    private final String bucket;
    private final String blobName;
    private final String filePathToCredentialsFile;

    public GoogleCloudStorageSource(String bucket, String blobName, String filePathToCredentialsFile, String inputUrl){
        this(bucket, blobName, filePathToCredentialsFile, inputUrl, "UTF-8");
    }


    public GoogleCloudStorageSource(String bucket, String blobName, String filePathToCredentialsFile, String inputUrl, String encoding) {
        super(DataSetType.createDefault(String.class));
        this.inputUrl = inputUrl;
        this.encoding = encoding;
        this.filePathToCredentialsFile =filePathToCredentialsFile;
        this.bucket = bucket;
        this.blobName = blobName;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public GoogleCloudStorageSource(GoogleCloudStorageSource that) {
        super(that);
        this.filePathToCredentialsFile =that.getfilePathToCredentialsFile();
        this.inputUrl = that.getInputUrl();
        this.encoding = that.getEncoding();
        this.bucket = that.getBucket();
        this.blobName =that.getBlobName();
    


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
        Validate.isTrue(GoogleCloudStorageSource.this.getNumInputs() == inputEstimates.length);

        // see Job for StopWatch measurements
        final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
        );

        // Query the job cache first to see if there is already an estimate.
        String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), GoogleCloudStorageSource.this.inputUrl);
        CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);

        if (cardinalityEstimate != null) return  cardinalityEstimate;

        // Otherwise calculate the cardinality.
        // First, inspect the size of the file and its line sizes.


        OptionalLong fileSize = getBlobByteSize();


        if (!fileSize.isPresent()) {
            GoogleCloudStorageSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                GoogleCloudStorageSource.this.inputUrl);
            timeMeasurement.stop();
            return this.FALLBACK_ESTIMATE;

        } else if (fileSize.getAsLong() == 0L) {
            timeMeasurement.stop();
            return new CardinalityEstimate(0L, 0L, 1d);
        }

        //TODO how to pass down blob name? Should maybe be in consutrctor?

        OptionalDouble bytesPerLine = this.estimateBytesPerLine();
        if (!bytesPerLine.isPresent()) {
            GoogleCloudStorageSource.this.logger.warn("Could not determine average line size of {}... deliver fallback estimate.",
                    GoogleCloudStorageSource.this.inputUrl);
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
            try{
                Blob blob = getBlob();
        
            if (blob == null || !blob.exists()) {
                return OptionalDouble.empty();
            }

            final int KiB = 1024;
            final int MiB = KiB * 1024; // 1 MiB


                try (LimitedInputStream lis = new LimitedInputStream(Channels.newInputStream(blob.reader()), 1 * MiB)) {

                    final BufferedReader bufferedReader = new BufferedReader(
                        new InputStreamReader(lis, GoogleCloudStorageSource.this.encoding)
                    );

                    // Read as much as possible.
                    char[] cbuf = new char[1024];
                    int numReadChars, numLineFeeds = 0;
                    while ((numReadChars = bufferedReader.read(cbuf)) != -1) {

                        System.out.println("PRINTING NUM READ CHARS: " + numReadChars);
                        
                        for (int i = 0; i < numReadChars; i++) {
                            if (cbuf[i] == '\n') {
                                System.out.println("PRINTING new line character: " + cbuf[i]);
                                numLineFeeds++;
                            }
                            System.out.println("PRINTING character: " + cbuf[i]);
                        }
                    }

                    if (numLineFeeds == 0) {
                        GoogleCloudStorageSource.this.logger.warn("Could not find any newline character in {}.", GoogleCloudStorageSource.this.inputUrl);
                        return OptionalDouble.empty();
                    }

                    return OptionalDouble.of((double) lis.getNumReadBytes() / numLineFeeds);


                }
            }

            catch (Exception e) {
                GoogleCloudStorageSource.this.logger.error("Could not estimate bytes per line of an input file.", e);
            }

        return OptionalDouble.empty();

        }
}    


    public OptionalLong getBlobByteSize() {
    try {
        
        return OptionalLong.of(getBlob().getSize());
    } 
    catch (Exception ex) {
        return OptionalLong.empty();
    }
    
    }
    public InputStream getInputStream() throws IOException { 
        return Channels.newInputStream(getBlob().reader());

    }

    public String getInputUrl() {
        return this.inputUrl;
    }

    public String getEncoding() {
        return this.encoding;
    }    

    public String getBucket() {
        return this.bucket;
    }

    public String getBlobName(){
        return this.blobName;
    }

    public String getfilePathToCredentialsFile(){
        return this.filePathToCredentialsFile;
    }

    public Blob getBlob() throws IOException{
        Storage storage = getStorage(getfilePathToCredentialsFile());
        return storage.get(bucket, blobName);
    }
    
    
    private static Storage getStorage(String filePathToCredentialsFile) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(filePathToCredentialsFile);
        ServiceAccountCredentials serviceAccountCredentiels = ServiceAccountCredentials.fromStream(fileInputStream);

        return StorageOptions.newBuilder()
            .setCredentials(serviceAccountCredentiels)
            .build()
            .getService();

    }
    
}
