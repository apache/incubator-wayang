package org.apache.wayang.basic.operators;

import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.commons.lang3.Validate;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.LimitedInputStream;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import org.json.JSONObject;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;


public class AmazonS3Source extends UnarySource<String> {
    private final Logger logger = LogManager.getLogger(this.getClass());

    //NOTE; TODO how is input url used? Or what is input url
    private final String inputUrl;

    private final String encoding;

    private final S3Client s3Client;
    private final String bucket;
    private final String blobName;

    public AmazonS3Source(String bucket, String blobName, String filePathToCredentialsFile, String inputUrl) throws IOException {
        this(bucket, blobName, filePathToCredentialsFile, inputUrl, "UTF-8");
    }
    

    public AmazonS3Source(String bucket, String blobName, String filePathToCredentialsFile, String inputUrl, String encoding) throws IOException {
        super(DataSetType.createDefault(String.class));
        this.inputUrl = inputUrl;
        this.encoding = encoding;

        this.s3Client = getS3Client(filePathToCredentialsFile);
        this.bucket = bucket;
        this.blobName = blobName;

        System.out.println("FOUND BUCKET! " + bucket);
    }


     /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */

    public AmazonS3Source(AmazonS3Source that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.encoding = that.getEncoding();
        this.bucket = that.getBucket();
        this.s3Client = that.getS3Client();
        this.blobName = that.getBlobName();
    }

        // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/GetObjectRequest.html
        private static GetObjectRequest getGetObjectRequest(String bucketName, String blobName ) {
            return GetObjectRequest.builder()
                .bucket(bucketName)
                .key(blobName)
                .build();
        }
    
    
        public String getInputUrl() {
            return inputUrl;
        }
    
    
        public String getEncoding() {
            return encoding;
        }
    
    
        public S3Client getS3Client() {
            return s3Client;
        }
    
    
        public String getBucket() {
            return bucket;
        }
    
        public String getBlobName() {
            return blobName;
        }

        //TODO ADDED TO BE ABLE TO run the GetEstemitesBytesPerLine. Delete when not used anymore.

        public OptionalDouble GetEstimateBytesPerLine() {

            ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getGetObjectRequest(bucket, blobName));    
            
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
                    AmazonS3Source.this.logger.warn("Could not find any newline character in {}.", AmazonS3Source.this.inputUrl);
                    return OptionalDouble.empty();
                }
    
                return OptionalDouble.of((double) lis.getNumReadBytes() / numLineFeeds);
            }
    
            catch (IOException e) {
                AmazonS3Source.this.logger.error("Could not estimate bytes per line of an input file.", e);
            }
    
            return OptionalDouble.empty();
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
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), AmazonS3Source.this.inputUrl);
            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);

            if (cardinalityEstimate != null) return  cardinalityEstimate;

            // Otherwise calculate the cardinality.
            // First, inspect the size of the file and its line sizes.

            //TODO: verify that filesize in FileSystems works with Cloud operator. Otherwise use built in method for Cloud operators to get filesize. Should we add to filesystems class or just use local method to get blob size. 
            //TODO: AWS and GOOGLE has built in.
            OptionalLong fileSize = FileSystems.getFileSize(AmazonS3Source.this.inputUrl);


            if (!fileSize.isPresent()) {
                AmazonS3Source.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                        AmazonS3Source.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;

            } else if (fileSize.getAsLong() == 0L) {
                timeMeasurement.stop();
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            //TODO how to pass down blob name? Should maybe be in consutrctor?

            OptionalDouble bytesPerLine = this.estimateBytesPerLine();
            if (!bytesPerLine.isPresent()) {
                AmazonS3Source.this.logger.warn("Could not determine average line size of {}... deliver fallback estimate.",
                        AmazonS3Source.this.inputUrl);
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



    private OptionalDouble estimateBytesPerLine() {

        ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getGetObjectRequest(bucket, blobName));    
        
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
                AmazonS3Source.this.logger.warn("Could not find any newline character in {}.", AmazonS3Source.this.inputUrl);
                return OptionalDouble.empty();
            }

            return OptionalDouble.of((double) lis.getNumReadBytes() / numLineFeeds);
        }

        catch (IOException e) {
            AmazonS3Source.this.logger.error("Could not estimate bytes per line of an input file.", e);
        }

        return OptionalDouble.empty();
    }
    }
        











    private static S3Client getS3Client(String filePathToCredentialsFile) throws IOException{
        String credentialsString = new String(
            Files.readAllBytes(
                Paths.get(filePathToCredentialsFile))
            ); 

        JSONObject credentialsJson = new JSONObject(credentialsString); 
        String accessKey = getAccessKey(credentialsJson); 
        String secretKey = getSecretKey(credentialsJson); 
        Region region = getRegion(credentialsJson); 
        
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create( AwsBasicCredentials.create(accessKey, secretKey) );
        
        return S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();


    }

    private static String getAccessKey(JSONObject credentialsJson){
        return credentialsJson.getString("accessKey");
    }

    private static String getSecretKey(JSONObject credentialsJson){
        return credentialsJson.getString("secretKey");
    }

    private static Region getRegion(JSONObject credentialsJson){
        String regionString = credentialsJson.getString("region");
        return Region.of(regionString);
    }





    
}
