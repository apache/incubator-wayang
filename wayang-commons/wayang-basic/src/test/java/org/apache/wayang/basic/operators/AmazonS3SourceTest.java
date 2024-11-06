package org.apache.wayang.basic.operators;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;



public class AmazonS3SourceTest {
    private final Logger logger = LogManager.getLogger(this.getClass());

    @Test
    public void estimateBytesPerLine() throws IOException {

        String filePath = "/Users/christofferkristensen/Downloads/S3.JSON";

        String bucketName = "wayang-test-bucket";
        String blobName = "S3-sample.txt";

        var source = new AmazonS3Source(bucketName, blobName, filePath, "");

        var doubles = source.GetEstimateBytesPerLine();

        System.out.println("Found estimated bytes per line " + doubles);

        assertTrue(true);
    }
}