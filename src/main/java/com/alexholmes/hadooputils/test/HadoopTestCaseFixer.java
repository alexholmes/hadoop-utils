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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

/**
 * Extends Hadoop's built-in {@link HadoopTestCase} to make it work outside
 * of Hadoop's build environment.
 */
public abstract class HadoopTestCaseFixer extends HadoopTestCase {

    /**
     * Constructor.
     *
     * @throws IOException if something goes wrong
     */
    public HadoopTestCaseFixer() throws IOException {
        super(HadoopTestCase.CLUSTER_MR, HadoopTestCase.DFS_FS, 1, 1);
    }

    /**
     * Creates a testcase for local or cluster MR using DFS.
     * <p/>
     * The DFS will be formatted regardless if there was one or not before in the
     * given location.
     *
     * @param mrMode       indicates if the MR should be local (LOCAL_MR) or cluster
     *                     (CLUSTER_MR)
     * @param fsMode       indicates if the FS should be local (LOCAL_FS) or DFS (DFS_FS)
     *                     <p/>
     *                     local FS when using relative PATHs)
     * @param taskTrackers number of task trackers to start when using cluster
     * @param dataNodes    number of data nodes to start when using DFS
     * @throws IOException thrown if the base directory cannot be set.
     */
    public HadoopTestCaseFixer(final int mrMode, final int fsMode,
                               final int taskTrackers, final int dataNodes) throws IOException {
        super(mrMode, fsMode, taskTrackers, dataNodes);
    }

    /**
     * Performs some additional setup required before the
     * {@link org.apache.hadoop.mapred.HadoopTestCase#setUp()} method can be called.
     *
     * @throws Exception if something goes wrong
     */
    @Override
    @Before
    public void setUp() throws Exception {
        // this path is used by
        File f = new File("build/test/mapred/local").getAbsoluteFile();
        if (f.exists()) {
            FileUtils.forceDelete(f);
        }
        FileUtils.forceMkdir(f);

        // required by JobHistory.initLogDir
        System.setProperty("hadoop.log.dir", f.getAbsolutePath());

        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }


}
