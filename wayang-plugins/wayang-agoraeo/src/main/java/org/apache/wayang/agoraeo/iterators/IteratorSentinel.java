package org.apache.wayang.agoraeo.iterators;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

public class IteratorSentinel extends IteratorSentinelDownload<String> {

    // Iterator receives a unique map
    public IteratorSentinel(String python_location, String downloader_location, Map<String, String> params) {
        // from params create command?, or prepare IteratorSentinelDownload to receive Map
        super(
            python_location,
            downloader_location,
            params
        );
    }

    @Override
    protected Stream<String> getLogic(Stream<String> baseline) {
        return baseline;
    }

    @Override
    protected String getDefaultValue() {
        return "";
    }
}
