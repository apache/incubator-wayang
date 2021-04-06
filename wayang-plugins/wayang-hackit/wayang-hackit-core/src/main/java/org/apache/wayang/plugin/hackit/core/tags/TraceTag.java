package org.apache.wayang.plugin.hackit.core.tags;

public class TraceTag extends HackitTag  {
    private static TraceTag seed = null;

    private TraceTag(){
        super();
    }

    @Override
    public boolean isSendOut() {
        return false;
    }

    @Override
    public boolean isSkip() {
        return false;
    }

    @Override
    public boolean isHaltJob() {
        return false;
    }

    @Override
    public boolean hasCallback() {
        return false;
    }

    @Override
    public HackitTag getInstance() {
        if(seed == null){
            seed = new TraceTag();
        }
        return seed;
    }

    @Override
    public int hashCode() {
        return 7;
    }
}
