package org.apache.wayang.plugin.hackit.core.tags;

public class CrashTag extends HackitTag {

    private static CrashTag seed = null;

    private CrashTag(){
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
            seed = new CrashTag();
        }
        return seed;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}
