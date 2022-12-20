package org.apache.wayang.agoraeo.operators.basic;

import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.wayang.agoraeo.WayangAgoraEO;
import org.apache.wayang.agoraeo.iterators.StringIteratorSentinelDownload;
import org.apache.wayang.agoraeo.sentinel.Mirror;
import org.apache.wayang.agoraeo.utilities.Utilities;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;

import java.io.*;

public class SentinelSource extends UnarySource<String> implements Serializable{

    protected String python_location;

    protected String module_location;
    protected Map<String, String> constant_variables;
    protected Map<String, List<String>> iterable_variables;
//    protected List<String> mirrors;
    protected List<Mirror> mirrors;

    public SentinelSource(){
        this(new HashMap<>(), new HashMap<>());
    }

    public SentinelSource (Map<String, String> constants, Map<String, List<String>> iterables){
        super(DataSetType.createDefault(String.class));
        this.constant_variables = constants;
        this.iterable_variables = iterables;


//        Configuration conf = new Configuration(WayangAgoraEO.DEFAULT_CONFIG_FILE);
//        conf.
        InputStream str = ReflectionUtils.loadResource(WayangAgoraEO.DEFAULT_CONFIG_FILE);


        try {
            final Properties properties = new Properties();
            properties.load(str);

            this.python_location = properties.getProperty("org.apache.wayang.agoraeo.python.location");

            this.module_location = properties.getProperty("org.apache.wayang.agoraeo.minimaldownload.location");

//            this.mirrors = Arrays.asList(properties.getProperty("org.apache.wayang.agoraeo.mirror").split(","));

//            this.constant_variables.put(
//                "user",
//                properties.getProperty("org.apache.wayang.agoraeo.user")
//            );
//            this.constant_variables.put(
//                "password",
//                properties.getProperty("org.apache.wayang.agoraeo.pass")
//            );

        } catch (IOException e) {
            throw new WayangException("Could not load configuration.", e);
        } finally {
            IOUtils.closeQuietly(str);
        }
    }

    public SentinelSource(SentinelSource that) {
        super(that);
        //TODO this need to be cloned not assigned :D
        this.constant_variables = that.constant_variables;
        this.iterable_variables = that.iterable_variables;
        this.python_location = that.python_location;
        this.module_location = that.module_location;
        this.mirrors = that.mirrors;
    }

    public Map<String, String> getConstants(){
        return this.constant_variables;
    }

    public Map<String, List<String>> getIterables(){
        return this.iterable_variables;
    }

    public String getConstant(String key){
        return this.constant_variables.get(key);
    }

    public SentinelSource setConstant(String key, String value){
        this.constant_variables.put(key, value);
        return this;
    }

    public List<String> getIterable(String key){
        return this.iterable_variables.get(key);
    }

    public SentinelSource setIterable(String key, List<String> value){
        this.iterable_variables.put(key, value);
        return this;
    }

    public String getFrom(){
        return this.getConstant("from");
    }

    public SentinelSource setFrom(String from ){
        return this.setConstant("from", from);
    }

    public String getTo(){
        return this.getConstant("to");
    }

    public SentinelSource setTo(String to){
        return this.setConstant("to", to);
    }
    public List<String> getOrder(){
        return this.getIterable("order");
    }

    public SentinelSource setOrder(List<String> orders){
        return this.setIterable("order", orders);
    }

    protected List<Map<String, String>> getCollection(){
        return Utilities.distribute(
                Utilities.flattenParameters(this.iterable_variables, this.constant_variables),
                "url",
                this.mirrors
        );
    }

    protected String getPython_location(){
        return this.python_location;
    }

    protected String getModule_location(){
        return this.module_location;
    }

    public List<Mirror> getMirrors() {
        return mirrors;
    }

    public SentinelSource setMirrors(List<Mirror> mirrors) {
        this.mirrors = mirrors;
        return this;
    }
}
