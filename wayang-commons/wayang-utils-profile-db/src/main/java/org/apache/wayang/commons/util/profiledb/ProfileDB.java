/*
 * Copyright 2016 Sebastian Kruse,
 * code from: https://github.com/sekruse/profiledb-java.git
 * Original license:
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.wayang.commons.util.profiledb;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.wayang.commons.util.profiledb.json.MeasurementDeserializer;
import org.apache.wayang.commons.util.profiledb.json.MeasurementSerializer;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.storage.Storage;

import java.io.*;
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

    /**
     * Receive an array of {@link Experiment}s and persist them
     *
     * @param experiments Array of {@link Experiment}s to be persisted
     * @throws IOException
     */
    public void save(Experiment... experiments) throws IOException {
        this.storage.save(experiments);
    }

    /**
     * Receive a Collection of {@link Experiment}s and persist them
     *
     * @param experiments Collection of {@link Experiment}s to be persisted
     * @throws IOException
     */
    public void save(Collection<Experiment> experiments) throws IOException {
        this.storage.save(experiments);
    }

    /**
     * Receive a Collection of {@link Experiment}s and persist them
     *
     * @param experiments Collection of {@link Experiment}s to be persisted
     * @param outputStream Indicates where the data must to be written
     * @throws IOException
     */
    public void save(Collection<Experiment> experiments, OutputStream outputStream) throws IOException {
        this.storage.save(experiments, outputStream);
    }

    /**
     * Related to file based storage, Receive an array of {@link Experiment}s and persist them at the end of a file
     *
     * @param experiments Array of {@link Experiment}s to be persisted
     * @throws IOException
     */
    public void append(Experiment... experiments) throws IOException {
        this.storage.append(experiments);
    }

    /**
     * Related to file based storage, Receive a Collection of {@link Experiment}s and persist them at the end of a file
     *
     * @param experiments Collection of {@link Experiment}s to be persisted
     * @throws IOException
     */
    public void append(Collection<Experiment> experiments) throws IOException {
        this.storage.append(experiments);
    }

    /**
     * Bring {@link Experiment}s from current Storage to local variable
     *
     * @return Collection of {@link Experiment}s
     * @throws IOException
     */
    public Collection<Experiment> load() throws IOException {
        return this.storage.load();
    }

    /**
     * Bring {@link Experiment}s from current Storage to local variable
     *
     * @param inputStream Data to be read
     * @return Collection of {@link Experiment}s
     * @throws IOException
     */
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
