/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.util.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;

/**
 * Abstraction for accessing a file system.
 */
public interface FileSystem {

    /**
     * Return the number of bytes of a given file.
     *
     * @param fileUrl URL that identifies the filed
     * @return the number of bytes of the file
     * @throws FileNotFoundException if the file could not be found
     */
    long getFileSize(String fileUrl) throws FileNotFoundException;

    /**
     * @return whether this instance is eligible to operate the file specified in the given {@code url}
     */
    boolean canHandle(String url);

    /**
     * Opens the file specified in the given {@code url}.
     *
     * @param url points to the file to be opened
     * @return an {@link InputStream} with the file's contents
     * @throws IOException if the file cannot be accessed properly for whatever reason
     */
    InputStream open(String url) throws IOException;

    /**
     * Opens the file specified in the given {@code url} for (over-)writing.
     *
     * @param url points to the file to be created
     * @return an {@link OutputStream} that allows writing to the specified file
     * @throws IOException if the file cannot be created properly for whatever reason
     */
    OutputStream create(String url) throws IOException;

    /**
     * Opens the file specified in the given {@code url} for (over-)writing.
     *
     * @param url points to the file to be created
     * @param forceCreateParentDirs if true, will create parent directories if they don't exist.
     * @return an {@link OutputStream} that allows writing to the specified file
     * @throws IOException if the file cannot be created properly for whatever reason
     */
    OutputStream create(String url, Boolean forceCreateParentDirs) throws IOException;

    boolean isDirectory(String url);

    Collection<String> listChildren(String url);

    /**
     * Deletes the given file at the given {@code url}. To delete directories, specify {@code isRecursiveDelete}.
     *
     * @return whether after the deletion, there is no more file associated with the given {@code url}
     */
    boolean delete(String url, boolean isRecursiveDelete) throws IOException;
}
