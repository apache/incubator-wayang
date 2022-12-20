package org.apache.wayang.agoraeo.operators.basic;

import org.apache.commons.io.IOUtils;
import org.apache.wayang.agoraeo.WayangAgoraEO;
import org.apache.wayang.agoraeo.iterators.StringIteratorSentinelDownload;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class SentinelSource extends UnarySource<String> implements Serializable{

    private Iterator<String> iterator;

    /**
     * Default construct
     *
     * @param order {@link String} what to do inside the command
     */
    public SentinelSource(String order){
        super(DataSetType.createDefault(String.class));

//        Configuration conf = new Configuration(WayangAgoraEO.DEFAULT_CONFIG_FILE);
//        conf.
        InputStream str = ReflectionUtils.loadResource(WayangAgoraEO.DEFAULT_CONFIG_FILE);

        String path_python = null;
        String downloader_location = null;
        String mirror = null;
        String user = null;
        String pass = null;

        try {
            final Properties properties = new Properties();
            properties.load(str);
            for (Map.Entry<Object, Object> propertyEntry : properties.entrySet()) {

                final String key = propertyEntry.getKey().toString();
                final String value = propertyEntry.getValue().toString();

                if(Objects.equals(key, "org.apache.wayang.agoraeo.python.location")){
                    path_python = value;
                }
                if(Objects.equals(key, "org.apache.wayang.agoraeo.minimaldownload.location")){
                    downloader_location = value;
                }
                if(Objects.equals(key, "org.apache.wayang.agoraeo.mirror")){
                    mirror = value;
                }
                if(Objects.equals(key, "org.apache.wayang.agoraeo.user")){
                    user = value;
                }
                if(Objects.equals(key, "org.apache.wayang.agoraeo.pass")){
                    pass = value;
                }

            }
        } catch (IOException e) {
            throw new WayangException("Could not load configuration.", e);
        } finally {
            IOUtils.closeQuietly(str);
        }

        if(path_python == null || downloader_location == null || mirror == null || user == null || pass == null){
            throw new WayangException("Missing inputs for Python Minimal Download");
        }

        String cmd = path_python + " " + downloader_location + " --url " + mirror + " --user " + user + " --password " + pass + " " + order;

        this.iterator = new StringIteratorSentinelDownload("Sentinel 2 - API", cmd);

//        throw new WayangException("ABORT ON PURPOSE");

    }

    public SentinelSource(SentinelSource that) {
        super(that);
        this.iterator = that.getIterator();
    }

    public Iterator<String> getIterator() {
        return this.iterator;
    }

}
