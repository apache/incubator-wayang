package profiledb;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import profiledb.json.MeasurementDeserializer;
import profiledb.json.MeasurementSerializer;
import profiledb.model.Experiment;
import profiledb.model.Measurement;
import profiledb.storage.Storage;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

/**
 * This class provides facilities to save and load {@link Experiment}s.
 */
public class ProfileDB {

    /**
     * Maintains the full list of {@link Class}es for {@link Measurement}s. Which are required for deserialization.
     */
    private List<Class<? extends Measurement>> measurementClasses = new LinkedList<>();

    /**
     * Controls how conducted experiments will be persisted and loaded
     */
    private Storage storage;

    /**
     * Maintains actions to preparate {@link Gson}.
     */
    private List<Consumer<GsonBuilder>> gsonPreparationSteps = new LinkedList<>();

    /**
     * Maintains a {@link Gson} object for efficiency. It will be dropped on changes, though.
     */
    private Gson gson;

    public void save(Experiment... experiments) throws IOException {
        this.storage.save(experiments);
    }

    public void save(Collection<Experiment> experiments) throws IOException {
        this.storage.save(experiments);
    }

    public void save(Collection<Experiment> experiments, OutputStream outputStream) throws IOException {
        this.storage.save(experiments, outputStream);
    }

    public void append(Experiment... experiments) throws IOException {
        this.storage.append(experiments);
    }

    public void append(Collection<Experiment> experiments) throws IOException {
        this.storage.append(experiments);
    }

    public Collection<Experiment> load() throws IOException {
        return this.storage.load();
    }

    public Collection<Experiment> load(InputStream inputStream) throws IOException {
        return this.storage.load(inputStream);
    }

    /**
     * Creates a new instance.
     */
    public ProfileDB(Storage storage) {

        this.storage = storage;
        this.storage.setContext(this);
        //this.measurementClasses.add(TimeMeasurement.class);
    }

    /**
     * To work with storage object provided to persist or load experiments
     *
     * @return Storage object proportioned for this instance
     */
    public Storage getStorage() {
        return storage;
    }

    /**
     * Register a {@link Measurement} type. This is required before being able to load that type.
     *
     * @param measurementClass the {@link Measurement} {@link Class}
     * @return this instance
     */
    public ProfileDB registerMeasurementClass(Class<? extends Measurement> measurementClass) {
        this.measurementClasses.add(measurementClass);
        this.gson = null;
        return this;
    }

    /**
     * Apply any changes necessary to {@link Gson} so that it can be used for de/serialization of custom objects.
     *
     * @param preparation a preparatory step performed on a {@link GsonBuilder}
     * @return this instance
     */
    public ProfileDB withGsonPreparation(Consumer<GsonBuilder> preparation) {
        this.gsonPreparationSteps.add(preparation);
        this.gson = null;
        return this;
    }

    /**
     * Provide a {@link Gson} object.
     *
     * @return the {@link Gson} object
     */
    public Gson getGson() {
        if (this.gson == null) {
            MeasurementSerializer measurementSerializer = new MeasurementSerializer();
            MeasurementDeserializer measurementDeserializer = new MeasurementDeserializer();
            this.measurementClasses.forEach(measurementDeserializer::register);
            final GsonBuilder gsonBuilder = new GsonBuilder()
                    .registerTypeAdapter(Measurement.class, measurementDeserializer)
                    .registerTypeAdapter(Measurement.class, measurementSerializer);
            this.gsonPreparationSteps.forEach(step -> step.accept(gsonBuilder));
            this.gson = gsonBuilder.create();
        }
        return this.gson;
    }

}
