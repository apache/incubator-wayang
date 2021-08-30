package profiledb.measurement;

import profiledb.model.Measurement;
import profiledb.model.Type;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;

/**
 * {@link Measurement} implementation for test purposes.
 */
@Type("test-time")
public class TestTimeMeasurement extends Measurement {

    private long millis;

    private Collection<Measurement> submeasurements;

    public TestTimeMeasurement(String id, long millis) {
        super(id);
        this.millis = millis;
        this.submeasurements = new LinkedList<>();
    }

    public long getMillis() {
        return millis;
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

    public Collection<Measurement> getSubmeasurements() {
        return submeasurements;
    }

    public void addSubmeasurements(TestTimeMeasurement submeasurements) {
        this.submeasurements.add(submeasurements);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TestTimeMeasurement that = (TestTimeMeasurement) o;
        return millis == that.millis &&
                Objects.equals(submeasurements, that.submeasurements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), millis, submeasurements);
    }
}