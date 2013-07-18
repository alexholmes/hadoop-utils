/*
 * Copyright 2012 Alex Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alexholmes.hadooputils.io;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

/**
 * Utilities to help work with files in Hadoop file systems.
 */
public final class FileUtils {

    /**
     * Private ctor to prevent instantiation.
     */
    private FileUtils() {
    }

    /**
     * Read the contents of the supplied file into a list.
     *
     * @param fs a Hadoop file system
     * @param p  the file path
     * @return array of lines in the file
     * @throws java.io.IOException if something goes wrong
     */
    public static List<String> readLines(final FileSystem fs, final Path p)
            throws IOException {
        InputStream stream = fs.open(p);
        try {
            return IOUtils.readLines(stream);
        } finally {
            stream.close();
        }
    }

    /**
     * Writes the array list into a file as newline-separated lines.
     *
     * @param fs a Hadoop file system
     * @param p  the file path
     * @return array of lines to write to the file
     * @throws java.io.IOException if something goes wrong
     */
    public static void writeLines(Collection<?> lines, final FileSystem fs, final Path p)
            throws IOException {
        OutputStream stream = fs.create(p);
        try {
            IOUtils.writeLines(lines, IOUtils.LINE_SEPARATOR, stream);
        } finally {
            stream.close();
        }
    }
}
