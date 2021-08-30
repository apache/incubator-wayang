package profiledb.measurement;

import profiledb.model.Measurement;
import profiledb.model.Type;

import java.util.Objects;

/**
 * {@link Measurement} implementation for test purposes.
 */
@Type("test-mem")
public class TestMemoryMeasurement extends Measurement {

    private long timestamp;

    private long usedMb;

    public TestMemoryMeasurement(String id, long timestamp, long usedMb) {
        super(id);
        this.timestamp = timestamp;
        this.usedMb = usedMb;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getUsedMb() {
        return usedMb;
    }

    public void setUsedMb(long usedMb) {
        this.usedMb = usedMb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TestMemoryMeasurement that = (TestMemoryMeasurement) o;
        return timestamp == that.timestamp &&
                usedMb == that.usedMb;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), timestamp, usedMb);
    }

    @Override
    public String toString() {
        return "TestMemoryMeasurement{" +
                "timestamp=" + timestamp +
                ", usedMb=" + usedMb +
                '}';
    }
}