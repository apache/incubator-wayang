package org.apache.wayang.agoraeo.iterators;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public class FileIteratorSentinelDownload extends IteratorSentinelDownload<File> {

    public FileIteratorSentinelDownload(String name, String command) {
        super(name, command);
    }

    @Override
    protected Stream<File> getLogic(Stream<String> baseline) {
        Collection<File> files = new ArrayList<>();
        Iterator<String> iter = baseline.iterator();
        for(; iter.hasNext(); ){
            String file = iter.next();
            files.add(new File(file));
        }
        return files.stream();
    }

    @Override
    protected File getDefaultValue() {
        return null;
    }
}
