package org.qcri.rheem.core.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link InputStream} that is trimmed to a specified size. Moreover, it counts the number of read bytes.
 */
public class LimitedInputStream extends InputStream {

    private final InputStream wrappedInputStream;

    private long numReadBytes = 0L;

    private final long maxReadBytes;

    /**
     * Create an unlimited instance.
     *
     * @param wrappedInputStream {@link InputStream} that is to be wrapped by this instance
     */
    public LimitedInputStream(InputStream wrappedInputStream) {
        this(wrappedInputStream, -1L);
    }

    /**
     * Create a limited instance.
     *
     * @param wrappedInputStream {@link InputStream} that is to be wrapped by this instance
     * @param maxReadBytes       maximum number of bytes to read from the stream
     */
    public LimitedInputStream(InputStream wrappedInputStream, long maxReadBytes) {
        this.wrappedInputStream = wrappedInputStream;
        this.maxReadBytes = maxReadBytes;
    }

    /**
     * @param len desired amount of bytes to read
     * @return the amount of bytes that can be read or {@code -1} if no bytes must be read any longer
     */
    private int getMaxBytesToRead(int len) {
        if (this.maxReadBytes != -1 && this.numReadBytes >= this.maxReadBytes) {
            return -1;
        }
        return (int) Math.min(len, this.maxReadBytes - this.numReadBytes);
    }

    @Override
    public int read() throws IOException {
        if (this.getMaxBytesToRead(1) == -1) {
            return -1;
        }
        final int b = this.wrappedInputStream.read();
        if (b != -1) {
            this.numReadBytes++;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int trimmedLen = this.getMaxBytesToRead(len);
        if (trimmedLen == -1) {
            return -1;
        }
        int numReadBytes = this.wrappedInputStream.read(b, off, trimmedLen);
        if (numReadBytes != -1) {
            this.numReadBytes += numReadBytes;
        }
        return numReadBytes;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n < 0) return 0;
        int trimmedN = this.getMaxBytesToRead((int) Math.min(Integer.MAX_VALUE, n));
        return this.wrappedInputStream.skip(trimmedN);
    }

    @Override
    public int available() throws IOException {
        return Math.max(0, this.getMaxBytesToRead(this.wrappedInputStream.available()));
    }

    @Override
    public void close() throws IOException {
        this.wrappedInputStream.close();
    }

    @Override
    public boolean markSupported() {
        // To support marks, we would need to keep track of marks during the counting.
        return false;
    }

    /**
     * @return the number of bytes that have been read from this stream
     */
    public long getNumReadBytes() {
        return this.numReadBytes;
    }
}
