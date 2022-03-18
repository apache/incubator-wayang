/*
 * Copyright 2016 Sebastian Kruse,
 * code from: https://github.com/sekruse/profiledb-java.git
 * Original license:
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.wayang.commons.util.profiledb.storage;

import org.apache.wayang.commons.util.profiledb.model.Experiment;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

public class JDBCStorage extends Storage {

    //TODO: Implement JDBC connection

    private File file;

    public JDBCStorage(URI uri) {
        super(uri);
        this.file = new File(uri);
    }

    @Override
    public void changeLocation(URI uri){

        super.changeLocation(uri);
        this.file = new File(uri);
    }

    /**
     * Write {@link Experiment}s to a {@link File}. Existing file contents will be overwritten.
     *
     * @param experiments the {@link Experiment}s
     * @throws IOException if the writing fails
     */
    @Override
    public void save(Collection<Experiment> experiments) throws IOException {
        this.file.getAbsoluteFile().getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(this.file, false)) {
            this.save(experiments, fos);
        }
    }

    /**
     * Write {@link Experiment}s to a {@link File}. Existing file contents will be overwritten.
     *
     * @param experiments the {@link Experiment}s
     * @throws IOException if the writing fails
     */
    @Override
    public void save(Experiment... experiments) throws IOException {
        this.save(Arrays.asList(experiments));
    }

    /**
     * Load {@link Experiment}s from a {@link File}.
     *
     * @return the {@link Experiment}s
     */
    @Override
    public Collection<Experiment> load() throws IOException {
        return load(new FileInputStream(this.file));
    }

    /**
     * Append {@link Experiment}s to a {@link File}. Existing file contents will be preserved.
     *
     * @param experiments the {@link Experiment}s
     * @throws IOException if the writing fails
     */
    @Override
    public void append(Collection<Experiment> experiments) throws IOException {
        this.file.getAbsoluteFile().getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(this.file, true)) {
            this.save(experiments, fos);
        }
    }

    /**
     * Append {@link Experiment}s to a {@link File}. Existing file contents will be preserved.
     *
     * @param experiments the {@link Experiment}s
     * @throws IOException if the writing fails
     */
    @Override
    public void append(Experiment... experiments) throws IOException {
        this.append(Arrays.asList(experiments));
    }
}
