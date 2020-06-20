package org.qcri.rheem.core.util;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * A mutable bit-mask.
 */
public class Bitmask implements Cloneable, Iterable<Integer> {

    /**
     * An instance without any bits set.
     */
    public static Bitmask EMPTY_BITMASK = new Bitmask(0);

    private static int BITS_PER_WORD = Long.BYTES * 8;

    private static int WORD_ADDRESS_BITS = 6;

    /**
     * Stores the actual bits.
     */
    private long[] bits;

    /**
     * Caches the cardinality or is {@code -1} if no values is cached.
     */
    private int cardinalityCache;

    /**
     * Creates a new instance.
     */
    public Bitmask() {
        this(BITS_PER_WORD);
    }

    /**
     * Creates a new instance.
     *
     * @param startCapacity the number of bits (starting from {@code 0}) that should already be held available
     */
    public Bitmask(int startCapacity) {
        int numLongs = startCapacity > 0 ? getLongPos(startCapacity - 1) + 1 : 0;
        this.bits = new long[numLongs];
        this.cardinalityCache = 0;
    }

    /**
     * Creates a new, copied instance.
     *
     * @param that the instance to copy
     */
    public Bitmask(Bitmask that) {
        this(that, that.bits.length << WORD_ADDRESS_BITS);
    }

    /**
     * Creates a new, copied instance.
     *
     * @param that          the instance to copy
     * @param startCapacity the number of bits (starting from {@code 0}) that should already be held available
     */
    public Bitmask(Bitmask that, int startCapacity) {
        this(Math.max(that.bits.length << WORD_ADDRESS_BITS, startCapacity));
        System.arraycopy(that.bits, 0, this.bits, 0, that.bits.length);
        this.cardinalityCache = that.cardinalityCache;
    }

    /**
     * Find the appropriate index to {@link #bits} for a given index.
     *
     * @param index of a bit
     * @return the index into {@link #bits}
     */
    private static int getLongPos(int index) {
        return index >>> WORD_ADDRESS_BITS;
    }

    /**
     * Find the appropriate offset in the appropriate element of {@link #bits} for a given index.
     *
     * @param index of a bit
     * @return the offset
     */
    private static int getOffset(int index) {
        return index & (BITS_PER_WORD - 1);
    }

    /**
     * Sets the bit at the given index.
     *
     * @param index where the bit should be set
     * @return whether this instance was changed
     */
    public boolean set(int index) {
        if (!this.ensureCapacity(index) && this.get(index)) {
            return false;
        }
        final int longPos = getLongPos(index);
        final int offset = getOffset(index);
        this.bits[longPos] = this.bits[longPos] | (1L << offset);
        if (this.cardinalityCache != -1) this.cardinalityCache++;
        return true;
    }

    /**
     * Gets the bit at the given index.
     *
     * @param index where the bit should be set
     * @return whether the bit in question is set
     */
    public boolean get(int index) {
        final int longPos = getLongPos(index);
        if (longPos >= this.bits.length) return false;
        final int offset = getOffset(index);
        return ((this.bits[longPos] >>> offset) & 1) != 0;
    }

    /**
     * Makes sure that {@link #bits} is large enough to comprise the given {@code index}.
     *
     * @param index an index for a bit
     * @return whether {@link #bits} was enlarged
     */
    private boolean ensureCapacity(int index) {
        int numRequiredLongs = getLongPos(index) + 1;
        if (this.bits.length < numRequiredLongs) {
            long[] newBits = new long[numRequiredLongs];
            System.arraycopy(this.bits, 0, newBits, 0, this.bits.length);
            this.bits = newBits;
            return true;
        }
        return false;
    }

    /**
     * Retrieves the number of set bits in this instance.
     *
     * @return the number of set bits
     */
    public int cardinality() {
        if (this.cardinalityCache == -1) {
            this.cardinalityCache = 0;
            for (long bits : this.bits) {
                this.cardinalityCache += Long.bitCount(bits);
            }
        }
        return this.cardinalityCache;
    }

    /**
     * Tells whether this instance is empty.
     *
     * @return whether this instance is empty
     */
    public boolean isEmpty() {
        return this.cardinality() == 0;
    }

    /**
     * Accumulates the given instance to this one via logical OR.
     *
     * @param that that should be accumulated
     * @return this instance
     */
    public Bitmask orInPlace(Bitmask that) {
        this.ensureCapacity(that.bits.length << WORD_ADDRESS_BITS);
        for (int i = 0; i < that.bits.length; i++) {
            this.bits[i] |= that.bits[i];
        }
        this.cardinalityCache = -1;
        return this;
    }

    /**
     * Creates the new instance that merges this and the given one via logical OR.
     *
     * @param that the other instance
     * @return this merged instance
     */
    public Bitmask or(Bitmask that) {
        Bitmask copy = new Bitmask(this, that.bits.length << WORD_ADDRESS_BITS);
        return copy.orInPlace(that);
    }


    /**
     * Accumulates the given instance to this one via logical AND.
     *
     * @param that that should be accumulated
     * @return this instance
     */
    public Bitmask andInPlace(Bitmask that) {
        this.ensureCapacity(that.bits.length << WORD_ADDRESS_BITS);
        for (int i = 0; i < that.bits.length; i++) {
            this.bits[i] &= that.bits[i];
        }
        for (int i = that.bits.length; i < this.bits.length; i++) {
            this.bits[i] = 0;
        }
        this.cardinalityCache = -1;
        return this;
    }

    /**
     * Creates the new instance that merges this and the given one via logical AND.
     *
     * @param that the other instance
     * @return this merged instance
     */
    public Bitmask and(Bitmask that) {
        Bitmask copy = new Bitmask(this, that.bits.length << WORD_ADDRESS_BITS);
        return copy.andInPlace(that);
    }

    /**
     * Accumulates the given instance to this one via logical AND NOT.
     *
     * @param that that should be accumulated
     * @return this instance
     */
    public Bitmask andNotInPlace(Bitmask that) {
        this.ensureCapacity(that.bits.length << WORD_ADDRESS_BITS);
        for (int i = 0; i < that.bits.length; i++) {
            this.bits[i] &= ~that.bits[i];
        }
        this.cardinalityCache = -1;
        return this;
    }

    /**
     * Creates the new instance that merges this and the given one via logical AND NOT.
     *
     * @param that the other instance
     * @return this merged instance
     */
    public Bitmask andNot(Bitmask that) {
        Bitmask copy = new Bitmask(this, that.bits.length << WORD_ADDRESS_BITS);
        return copy.andNotInPlace(that);
    }

    /**
     * Flips all bits in the given range
     *
     * @param fromIndex inclusive start index of the range
     * @param toIndex   exclusive end index of the range
     * @return this instance
     */
    public Bitmask flip(int fromIndex, int toIndex) {
        if (fromIndex < toIndex) {
            this.ensureCapacity(toIndex - 1);
            int fromLongPos = getLongPos(fromIndex);
            int untilLongPos = getLongPos(toIndex - 1);
            for (int longPos = fromLongPos; longPos <= untilLongPos; longPos++) {
                int fromOffset = (longPos == fromLongPos) ? getOffset(fromIndex) : 0;
                int untilOffset = (longPos == untilLongPos) ? getOffset(toIndex - 1) + 1 : BITS_PER_WORD;
                long flipMask = createAllSetBits(untilOffset) ^ createAllSetBits(fromOffset);
                this.bits[longPos] ^= flipMask;
            }
            this.cardinalityCache = -1;
        }
        return this;
    }

    /**
     * Creates a {@code long} that has the lowest {@code n} bits set.
     *
     * @param n the number of bits to be set
     * @return the {@code long}
     */
    private static long createAllSetBits(int n) {
        return n == BITS_PER_WORD ? -1L : (1L << n) - 1L;
    }

    /**
     * Checks whether all bits set in this instance are also set in the given instance.
     *
     * @param that the potential supermask
     * @return whether this is a submask
     */
    public boolean isSubmaskOf(Bitmask that) {
        final int minBitsLength = Math.min(this.bits.length, that.bits.length);
        for (int i = 0; i < minBitsLength; i++) {
            if (this.bits[i] != (this.bits[i] & that.bits[i])) return false;
        }
        for (int i = minBitsLength; i < this.bits.length; i++) {
            if (this.bits[i] != 0L) return false;
        }
        return true;
    }

    /**
     * Checks whether all bits set in this instance are not set in the given instance.
     *
     * @param that the potential disjoint instance
     * @return whether the instances are disjoint
     */
    public boolean isDisjointFrom(Bitmask that) {
        final int minBitsLength = Math.min(this.bits.length, that.bits.length);
        for (int i = 0; i < minBitsLength; i++) {
            if ((this.bits[i] & that.bits[i]) != 0) return false;
        }
        return true;
    }

    /**
     * Finds the next set bit, starting from a given index.
     *
     * @param from index from that the search starts
     * @return the index of the set next bit or {@code -1} if there is no next set bit
     */
    public int nextSetBit(int from) {
        int longPos = getLongPos(from);
        int offset = getOffset(from);
        while (longPos < this.bits.length) {
            long bits = this.bits[longPos];
            int nextOffset = Long.numberOfTrailingZeros(bits & ~createAllSetBits(offset));
            if (nextOffset < BITS_PER_WORD) {
                return longPos << WORD_ADDRESS_BITS | nextOffset;
            }
            longPos++;
            offset = 0;
        }
        return -1;
    }

    @Override
    public PrimitiveIterator.OfInt iterator() {
        return new PrimitiveIterator.OfInt() {

            private int next = Bitmask.this.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return this.next != -1;
            }

            @Override
            public int nextInt() {
                if (!this.hasNext()) throw new NoSuchElementException();
                int result = this.next;
                this.next = Bitmask.this.nextSetBit(this.next + 1);
                return result;
            }

            @Override
            public Integer next() {
                return this.nextInt();
            }

        };
    }

    @Override
    public Spliterator.OfInt spliterator() {
        return Spliterators.spliteratorUnknownSize(this.iterator(), 0);
    }

    public IntStream stream() {
        return StreamSupport.intStream(this.spliterator(), false);
    }

    @Override
    public String toString() {
        return this.toIndexString();
    }

    @SuppressWarnings("unused")
    private String toHexString() {
        StringBuilder sb = new StringBuilder(2 + this.bits.length * 8);
        sb.append("0x");
        for (long bits : this.bits) {
            sb.append(String.format("%016x", bits));
        }
        return sb.toString();
    }

    @SuppressWarnings("unused")
    private String toBinString() {
        StringBuilder sb = new StringBuilder(2 + this.bits.length * 64);
        sb.append("[");
        for (long bits : this.bits) {
            for (int i = 0; i < Long.BYTES * 8; i++) {
                sb.append(((bits >> i) & 1L) == 0 ? "0" : "1");
            }
        }
        return sb.append(']').toString();
    }

    private String toIndexString() {
        StringBuilder sb = new StringBuilder(2 + this.cardinality() * 8);
        sb.append("{");
        String separator = "";
        int nextIndex = this.nextSetBit(0);
        while (nextIndex != -1) {
            sb.append(separator).append(nextIndex);
            separator = ", ";
            nextIndex = this.nextSetBit(nextIndex + 1);
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bitmask that = (Bitmask) o;
        if (this.cardinalityCache != -1 && that.cardinalityCache != -1 && this.cardinalityCache != that.cardinalityCache) {
            return false;
        }
        final Bitmask smallInstance;
        final Bitmask largeInstance;
        if (this.bits.length < that.bits.length) {
            smallInstance = this;
            largeInstance = that;
        } else {
            smallInstance = that;
            largeInstance = this;
        }
        for (int i = 0; i < smallInstance.bits.length; i++) {
            if (smallInstance.bits[i] != largeInstance.bits[i]) return false;
        }
        for (int i = smallInstance.bits.length; i < largeInstance.bits.length; i++) {
            if (largeInstance.bits[i] != 0L) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int accu = 0;
        for (long bits : this.bits) {
            accu ^= bits ^ (bits >>> 32);
        }
        return accu;
    }
}
