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
import org.junit.After;
import org.junit.Before;

import java.io.File;

/**
 * Allows easy use of the {@link MiniHadoop} in JUnit test cases.
 */
public abstract class MiniHadoopTestCase {

    /**
     * The mini Hadoop.
     */
    private MiniHadoop miniHadoop;

    /**
     * Create a {@link MiniHadoop}, which in turn creates an in-memory distributed file system
     * and JobTracker.
     *
     * @throws Exception if something goes wrong
     */
    @Before
    public void setUp() throws Exception {
        miniHadoop = new MiniHadoop(new Configuration(), 1, 1, getTestDataDir());
    }

    /**
     * Called after every test method, and shuts down the in-memory distributed file system
     * and JobTracker.
     *
     * @throws Exception if something goes wrong
     */
    @After
    public void tearDown() throws Exception {
        miniHadoop.close();
    }

    /**
     * Returns the temporary data directory.
     *
     * @return the temp directory
     */
    public static File getTestDataDir() {
        return new File(System.getProperty("test.build.data", "build/test"));
    }

    /**
     * Gets the mini Hadoop.
     *
     * @return the mini Hadoop
     */
    public MiniHadoop getMiniHadoop() {
        return miniHadoop;
    }
}
