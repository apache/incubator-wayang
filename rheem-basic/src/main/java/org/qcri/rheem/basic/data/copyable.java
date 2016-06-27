package org.qcri.rheem.basic.data;

import java.io.Serializable;

/**
 * Created by khayyzy on 6/21/16.
 */
public abstract class copyable implements Serializable{
    public abstract copyable copy();
}
