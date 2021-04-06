package org.apache.wayang.plugin.hackit.core.tags;

public class SkipTag extends HackitTag  {
    private static SkipTag seed = null;

    private SkipTag(){
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
            seed = new SkipTag();
        }
        return seed;
    }

    @Override
    public int hashCode() {
        return 6;
    }
}
