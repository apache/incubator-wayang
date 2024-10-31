package org.apache.wayang.basic.operators;


import com.google.cloud.storage.Bucket;
import com.google.api.client.json.Json;
import com.google.api.services.storage.Storage.Projects.ServiceAccount;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.OptionalDouble;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.nio.channels.Channels;
public class GoogleTextFileSource extends UnarySource<String> {

    private final Logger logger = LogManager.getLogger(this.getClass());

    //TODO is this needed?
    private final String inputUrl;

    private final String encoding;
    private final Storage storage;
    private final Bucket bucket;

    public GoogleTextFileSource(String bucket, String filePathToCredentialsFile, String inputUrl) throws IOException {
        this(bucket, filePathToCredentialsFile, inputUrl, "UTF-8");
    }


    public GoogleTextFileSource(String bucket, String filePathToCredentialsFile, String inputUrl, String encoding) throws IOException {
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
    public GoogleTextFileSource(GoogleTextFileSource that) {
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
                new InputStreamReader(lis, GoogleTextFileSource.this.encoding)
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
                GoogleTextFileSource.this.logger.warn("Could not find any newline character in {}.", GoogleTextFileSource.this.inputUrl);
                return OptionalDouble.empty();
            }

            return OptionalDouble.of((double) lis.getNumReadBytes() / numLineFeeds);


        }
        catch (IOException e) {
            GoogleTextFileSource.this.logger.error("Could not estimate bytes per line of an input file.", e);
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
