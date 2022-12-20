package org.apache.wayang.agoraeo.iterators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Iterator;
import java.util.stream.Stream;

public abstract class IteratorSentinelDownload<Input> implements Iterator<Input>, Serializable {

    private String _name = "";
    private String command = "";
    private Process process = null;
    private Iterator<Input> iteratorProcess = null;

    public IteratorSentinelDownload(String name, String command) {
        this.command = command;
        this._name = name;
    }

    @Override
    public boolean hasNext() {
        if(this.process == null){
            return startProcess();
        }
        if( !this.process.isAlive() ){
            return this.iteratorProcess.hasNext();
        }
        return true;
    }

    @Override
    public Input next() {
        if( ! this.iteratorProcess.hasNext()){
            return this.getDefaultValue();
        }
        return this.iteratorProcess.next();
    }

    private boolean startProcess(){
        try {
            final String name = this._name;
            this.process = Runtime.getRuntime().exec(this.command);
            this.iteratorProcess = getLogic(
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getInputStream()
                            )
                    ).lines()
            ).iterator();
            return true;
        } catch (IOException e) {}
        return false;
    }

    protected abstract Stream<Input> getLogic(Stream<String> baseline);

    protected abstract Input getDefaultValue();

    protected String getName(){
        return this._name;
    }
}
