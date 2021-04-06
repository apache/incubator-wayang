package org.apache.wayang.plugin.hackit.core.tuple.header;

public class HeaderLong extends Header<Long> {
    static long base;

    static{
        base = 0;//(new Random()).nextLong();
    }

    public HeaderLong(Long id){
        super(id);
    }

    public HeaderLong() {
        super();
    }

    public HeaderLong(Long id, int child) {
        super(id, child);
    }

    @Override
    public HeaderLong createChild() {
        return new HeaderLong(this.getId(), this.child++);
    }

    @Override
    protected Long generateID() {
        return base++;
    }
}
