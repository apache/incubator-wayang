package profiledb.storage;

import com.google.gson.Gson;
import profiledb.ProfileDB;
import profiledb.model.Experiment;

import java.io.*;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Controls how conducted experiments will be persisted and loaded
 */
public abstract class Storage {

    /**
     * Object or URI where experiments are persisted
     */
    private URI storageFile;

    /**
     * To access profileDB general serialization functions
     */
    private ProfileDB context;

    /**
     * Creates a new instance.
     * @param uri Object or URI where experiments are persisted
     */
    public Storage(URI uri){
        this.storageFile = uri;
    }

    /**
     * Sets the ProfileDB for this instance that manages all the Measurement subclasses
     * */
    public void setContext(ProfileDB context) {
        this.context = context;
    }

    /**
     * Allows to change where future experiments will be persisted and loaded
     * @param uri
     */
    public void changeLocation(URI uri){
        this.storageFile = uri;
    }

    public void save(Experiment... experiments) throws IOException {}

    public void save(Collection<Experiment> experiments) throws IOException {}

    public void append(Experiment... experiments) throws IOException {}

    public void append(Collection<Experiment> experiments) throws IOException {}

    public Collection<Experiment> load() throws IOException { return null; }

    /**
     * Write {@link Experiment}s to an {@link OutputStream}.
     *
     * @param outputStream the {@link OutputStream}
     */
    public void save(Collection<Experiment> experiments, OutputStream outputStream) throws IOException {
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            this.save(experiments, writer);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpectedly, UTF-8 is not supported.");
        }
    }

    /**
     * Write {@link Experiment}s to a {@link Writer}.
     *
     * @param writer the {@link Writer}
     */
    public void save(Collection<Experiment> experiments, Writer writer) throws IOException {
        try {
            Gson gson = context.getGson();
            for (Experiment experiment : experiments) {
                gson.toJson(experiment, writer);
                writer.append('\n');
            }
            writer.flush();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpectedly, UTF-8 is not supported.");
        }
    }

    /**
     * Load {@link Experiment}s from an {@link InputStream}.
     *
     * @param inputStream the {@link InputStream}
     * @return the {@link Experiment}s
     */
    public Collection<Experiment> load(InputStream inputStream) throws IOException {
        try {
            return load(new BufferedReader(new InputStreamReader(inputStream, "UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unexpectedly, UTF-8 is not supported.");
        }
    }

    /**
     * Load {@link Experiment}s from an {@link Reader}.
     *
     * @param reader the {@link Reader}
     * @return the {@link Experiment}s
     */
    public Collection<Experiment> load(BufferedReader reader) throws IOException {
        Collection<Experiment> experiments = new LinkedList<>();
        Gson gson = context.getGson();
        String line;
        while ((line = reader.readLine()) != null) {
            Experiment experiment = gson.fromJson(line, Experiment.class);
            experiments.add(experiment);
        }
        return experiments;
    }
}
