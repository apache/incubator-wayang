package org.qcri.rheem.core.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Utility to expose interfaces that accept a callback as an {@link Iterator}.
 * <p>This class uses a lock-free ring buffer to achieve a high throughput and minimize stall times. Note that
 * the producer and consumer should run in different threads, otherwise deadlocks might occur.</p>
 * <p>The producer obtains a {@link Consumer} via {@link #getConsumer()} and pushes elements to it. When all
 * elements are pushed, {@link #declareLastAdd()} should be called. The consumer obtains a {@link Iterator} via
 * {@link #getIterator()} from that previously pushed elements can be obtained. Both operators can block when the
 * buffer is full or empty.</p>
 */
public class ConsumerIteratorAdapter<T> {

    /**
     * Sleeping time when a read/write to the {@link #ringBuffer} cannot be served.
     */
    private static final long SLEEP_MILLIS = 0L;

    /**
     * Default capacity for the {@link #ringBuffer}.
     */
    private static final int DEFAULT_CAPACITY = 1 << 16; // = 65,536

    /**
     * Buffers elements between the producer and consumer.
     */
    private final ArrayList<T> ringBuffer;

    /**
     * Maintains read and write pointers in the {@link #ringBuffer}.
     * <ul>
     * <li>The upper 32 bits represent the position to which the next write should be done.</li>
     * <li>The lower 32 bits represent the position on which the next read should be performed.</li>
     * </ul>
     * If read and write position are equal, the buffer is empty. If the write position is just ahead of the read
     * position, the buffer is full.
     */
    private final AtomicLong state;

    /**
     * Whether new writes can occur.
     */
    private boolean isWriteFinished = false;

    /**
     * Bitmask of relevant bits for both read and write positions.
     */
    private final int stateBits;

    /**
     * The {@link Iterator} for the consumer.
     */
    private final Iterator<T> iterator = new Iterator<T>() {

        private boolean isInitialized = false;

        private T next;

        private void ensureInitialized() {
            // We need to lazy-intialize to prevent deadlocks on instantiation.
            if (!this.isInitialized) {
                this.moveToNext();
                this.isInitialized = true;
            }
        }

        @Override
        public boolean hasNext() {
            this.ensureInitialized();
            return this.next != null;
        }

        @Override
        public T next() {
            this.ensureInitialized();
            T curNext = this.next;
            this.moveToNext();
            return curNext;
        }

        private void moveToNext() {
            this.next = ConsumerIteratorAdapter.this.read();
        }
    };

    /**
     * The {@link Consumer} for the producer.
     */
    private final Consumer<T> consumer = this::add;

    /**
     * Creates a new instance of capacity {@value DEFAULT_CAPACITY}.
     */
    public ConsumerIteratorAdapter() {
        this(DEFAULT_CAPACITY);
    }


    /**
     * Creates a new instance.
     *
     * @param minCapacity the minimum capacity of the buffer
     */
    public ConsumerIteratorAdapter(int minCapacity) {
        int numTrailingZeros = Integer.BYTES * 8 - Integer.numberOfLeadingZeros(minCapacity) - 1;
        int capacity = 1 << numTrailingZeros;
        if (capacity != minCapacity) {
            capacity <<= 1;
        }
        this.ringBuffer = new ArrayList<>(capacity);
        for (int i = 0; i < capacity; i++) {
            this.ringBuffer.add(null);
        }
        this.stateBits = capacity - 1;
        this.state = new AtomicLong(0);
    }

    /**
     * Retrieve the consumer interface.
     *
     * @return the consumer {@link Iterator}
     */
    public Iterator<T> getIterator() {
        return this.iterator;
    }

    /**
     * Retrieve the producer interface.
     *
     * @return the producer {@link Consumer}
     */
    public Consumer<T> getConsumer() {
        return this.consumer;
    }

    /**
     * Adds a new element to the {@link #ringBuffer}.
     *
     * @param element that should be added
     */
    private void add(T element) {
        assert !this.isWriteFinished;
        int writePos, nextWritePos;
        while (true) {
            final long state = this.state.get();
            final int readPos = (int) state;
            writePos = (int) (state >>> 32);

            // We permit to write if the next writable position is not
            nextWritePos = (writePos + 1) & this.stateBits;
            if (nextWritePos == readPos) {
                // If cannot read, wait to try again.
                if (SLEEP_MILLIS > 0) {
                    try {
                        Thread.sleep(SLEEP_MILLIS);
                    } catch (InterruptedException ignored) {
                    }
                }
            } else break;
        }
        // Add the element.
        this.ringBuffer.set(writePos, element);

        // Commit the updated write position.
        long delta = (long) (nextWritePos - writePos) << 32;
        this.state.addAndGet(delta);
    }

    /**
     * Retrieves an element from the {@link #ringBuffer}.
     *
     * @return the element or {@code null} if all elements have been read and no more elements are to appear
     */
    private T read() {
        int readPos;
        while (true) {
            final long state = this.state.get();
            readPos = (int) state;
            final int writePos = (int) (state >>> 32);

            if (readPos == writePos) {
                if (this.isWriteFinished) {
                    return null;
                } else {
                    // If cannot read, wait to try again.
                    if (SLEEP_MILLIS > 0) {
                        try {
                            Thread.sleep(SLEEP_MILLIS);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            } else break;
        }

        // Read the element.
        final T t = this.ringBuffer.get(readPos);

        // Commit the updated read position.
        int nextReadPos = (readPos + 1) & this.stateBits;
        long delta = (long) (nextReadPos - readPos);
        this.state.addAndGet(delta);

        return t;
    }

    /**
     * Declare that all elements have been pushed to the producer {@link Consumer}.
     *
     * @see #getConsumer()
     */
    public void declareLastAdd() {
        this.isWriteFinished = true;
    }
}
