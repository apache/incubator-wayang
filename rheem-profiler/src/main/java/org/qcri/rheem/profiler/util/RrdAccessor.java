package org.qcri.rheem.profiler.util;

import org.rrd4j.ConsolFun;
import org.rrd4j.core.FetchData;
import org.rrd4j.core.FetchRequest;
import org.rrd4j.core.RrdBackendFactory;
import org.rrd4j.core.RrdDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Utility to read from an RRD file.
 */
public class RrdAccessor implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final RrdDb rrdDb;

    private RrdAccessor(RrdDb rrdDb) {
        this.rrdDb = rrdDb;
        try {
            this.logger.info("Opened RRD with {} archives and data sources {}.", rrdDb.getArcCount(), Arrays.toString(rrdDb.getDsNames()));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static RrdAccessor open(String rrdPath) {
        try {
            final RrdDb rrdDb = new RrdDb("", "rrdtool:/" + rrdPath, RrdBackendFactory.getFactory("MEMORY"));
            rrdDb.getLastUpdateTime();
            return new RrdAccessor(rrdDb);
        } catch (IOException e) {
            throw new RuntimeException("Could not open RRD.", e);
        }
    }

    public double query(String dsName, long startTimestamp, long endTimestamp, ConsolFun consolidationFunction) {
        long tStart = convertToUnixTimestamp(startTimestamp);
        long tEnd = convertToUnixTimestamp(endTimestamp);
        if (tStart == tEnd) {
            this.logger.warn("Shifting end time by 1 second because it is identical with the start time.");
            tEnd++;
        }
        try {
            final FetchRequest request = this.rrdDb.createFetchRequest(consolidationFunction, tStart, tEnd);
            final FetchData fetchData = request.fetchData();
            return fetchData.getAggregate(dsName, consolidationFunction);
        } catch (IOException e) {
            e.printStackTrace();
            return Double.NaN;
        }
    }

    public long getLastUpdateMillis() {
        try {
            return convertToJavaTimestamp(this.rrdDb.getLastUpdateTime());
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static long convertToUnixTimestamp(long javaTimestamp) {
        return (javaTimestamp + 500) / 1000;
    }

    public static long convertToJavaTimestamp(long unixTimestamp) {
        return unixTimestamp * 1000 + 500;
    }

    @Override
    public void close() {
        if (this.rrdDb != null) {
            try {
                this.rrdDb.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
