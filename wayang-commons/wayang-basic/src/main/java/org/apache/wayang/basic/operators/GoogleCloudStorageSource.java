package org.apache.wayang.basic.operators;


import com.google.cloud.storage.Bucket;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import org.apache.wayang.core.plan.wayangplan.UnarySource;


import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.LimitedInputStream;

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
    private final Storage storage;
    private final Bucket bucket;

    public GoogleCloudStorageSource(String bucket, String filePathToCredentialsFile, String inputUrl) throws IOException {
        this(bucket, filePathToCredentialsFile, inputUrl, "UTF-8");
    }


    public GoogleCloudStorageSource(String bucket, String filePathToCredentialsFile, String inputUrl, String encoding) throws IOException {
        super(DataSetType.createDefault(String.class));
        this.inputUrl = inputUrl;
        this.encoding = encoding;

        this.storage = getStorage(filePathToCredentialsFile);

        this.bucket = storage.get(bucket);
    }

        //throw an error if bucket does not exists
    //     if (bucket == null) {
    //         throw new Exception("Can not connect to bucket, bucket is not found with name " + bucket);
    //     }
    // }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public GoogleCloudStorageSource(GoogleCloudStorageSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.encoding = that.getEncoding();
        this.bucket = that.getBucket();
        this.storage = that.getStorage();

        //throw an error if bucket does not exists
        //     if (bucket == null) {
        //         throw new Exception("Can not connect to bucket, bucket is not found with name " + bucket);
        //     }
        // }

    }

    // @Override
    // public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
    //         final int outputIndex,
    //         final Configuration configuration) {
    //     Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
    //     return Optional.of(new GoogleTextFileSource.CardinalityEstimator());
    // }

    public OptionalDouble GetEstimateBytesPerLine(String blobName) {
        return this.estimateBytesPerLine(blobName);
    }

            //TODO implement for google
    public OptionalLong getBlobByteSize() { return null;

        /*
    try {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
        .bucket(getBucket())
        .key(getBlobName())
        .build();

    HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);
    return OptionalLong.of(headObjectResponse.contentLength()); // returns the size in bytes
    } 
    catch (Exception ex) {
        return OptionalLong.empty();
    }

     */
    
    }

    //TODO needs this for both cloud opeartors
    public InputStream getInputStream() { 
        /*
        return s3Client.getObject(getGetObjectRequest(bucket, blobName));
         */
        return null;
    }

    /**
     * Estimate the number of bytes that are in each line of a given file.
     *
     * @return the average number of bytes per line if it could be determined
     */
    private OptionalDouble estimateBytesPerLine(String blobName) {
        Blob blob = bucket.get(blobName);

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
        catch (IOException e) {
            GoogleCloudStorageSource.this.logger.error("Could not estimate bytes per line of an input file.", e);
        }

        return OptionalDouble.empty();

    }

    public String getInputUrl() {
        return this.inputUrl;
    }

    public String getEncoding() {
        return this.encoding;
    }    

    public Storage getStorage(){
        return this.storage;
    }

    public Bucket getBucket() {
        return this.bucket;
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
