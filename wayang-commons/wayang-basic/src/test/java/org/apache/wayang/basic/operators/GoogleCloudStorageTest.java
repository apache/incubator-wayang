package org.apache.wayang.basic.operators;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class GoogleCloudStorageTest {

    private final Logger logger = LogManager.getLogger(this.getClass());

    @Test
    public void estimateBytesPerLine() throws IOException {

        String filePath = "/Users/christofferkristensen/Documents/SoftwareDesign/research_project/ResearchProject/long-centaur-438410-p7-90933b1671ea.json";

    //     String bucketName = "wayang-test-bucket";
    //     String blobName = "GCS-sample.txt";

    //     var source = new GoogleCloudStorageSource(bucketName, filePath, "");

    //     var doubles = source.GetEstimateBytesPerLine(blobName);

    //     System.out.println("Found estimated bytes per line " + doubles);

    //     assertTrue(true);
    // }
    }
}
