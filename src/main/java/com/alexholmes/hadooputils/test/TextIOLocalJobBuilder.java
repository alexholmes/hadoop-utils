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

package com.alexholmes.hadooputils.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * A class that helps with testing MapReduce jobs with the
 * {@link org.apache.hadoop.mapred.LocalJobRunner}, which is an in-memory
 * MapReduce implementation.
 * <p/>
 * See {@link TextIOJobBuilder} for more details.
 */
public class TextIOLocalJobBuilder extends TextIOJobBuilder {

    /**
     * Constructor, instantiates input/output paths, and sets some configuration items in the
     * supplied {@link org.apache.hadoop.conf.Configuration} to force the in-memory {@link
     * org.apache.hadoop.mapred.LocalJobRunner} to be leveraged when running a MapReduce job.
     *
     * @param config      the Hadoop configuration
     * @param localTmpDir the temp directory within which input/output directories and files are
     *                    created
     * @throws java.io.IOException if something goes wrong
     */
    public TextIOLocalJobBuilder(final Configuration config, final File localTmpDir)
            throws IOException {
        super(addLocalJobRunnerConfig(config),
                new Path(localTmpDir.getAbsolutePath(), "input"),
                new Path(localTmpDir.getAbsolutePath(), "output"));
    }

    /**
     * Augments the supplied Configuration object with settings to force the {@link
     * org.apache.hadoop.mapred.LocalJobRunner} to run.
     *
     * @param config the Hadoop configuration
     * @return the Hadoop configuration
     */
    public static Configuration addLocalJobRunnerConfig(final Configuration config) {
        config.set("mapred.job.tracker", "local");
        config.set("fs.default.name", "file:///");
        return config;
    }
}
