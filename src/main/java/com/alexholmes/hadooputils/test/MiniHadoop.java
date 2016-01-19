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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 * Wraps Hadoop's built-in {@link MiniMRCluster} and and {@link MiniDFSCluster}
 * to make them work outside of Hadoop's build environment.
 */
public class MiniHadoop {

    /**
     * The in-memory HDFS cluster.
     */
    private MiniDFSCluster dfsCluster = null;

    /**
     * The in-memory MapReduce cluster.
     */
    private MiniMRCluster mrCluster = null;

    /**
     * A handle to the in-memory HDFS.
     */
    private FileSystem fileSystem = null;

    /**
     * Creates a {@link MiniMRCluster} and {@link MiniDFSCluster} all working
     * within the directory supplied in {@code tmpDir}.
     *
     * The DFS will be formatted regardless if there was one or not before in
     * the given location.
     *
     * @param config
     *            the Hadoop configuration
     * @param taskTrackers
     *            number of task trackers to start
     * @param dataNodes
     *            number of data nodes to start
     * @param tmpDir
     *            the temporary directory which the Hadoop cluster will use for
     *            storage
     * @throws IOException
     *             thrown if the base directory cannot be set.
     */
    public MiniHadoop(final Configuration config, final int taskTrackers, final int dataNodes, final File tmpDir)
            throws IOException {

        if (taskTrackers < 1) {
            throw new IllegalArgumentException("Invalid taskTrackers value, must be greater than 0");
        }
        if (dataNodes < 1) {
            throw new IllegalArgumentException("Invalid dataNodes value, must be greater than 0");
        }

        config.set("hadoop.tmp.dir", tmpDir.getAbsolutePath());

        if (tmpDir.exists()) {
            FileUtils.forceDelete(tmpDir);
        }
        FileUtils.forceMkdir(tmpDir);

        // used by MiniDFSCluster for DFS storage
        System.setProperty("test.build.data", new File(tmpDir, "data").getAbsolutePath());

        // required by JobHistory.initLogDir
        System.setProperty("hadoop.log.dir", new File(tmpDir, "logs").getAbsolutePath());

        JobConf jobConfig = new JobConf(config);

        // Workaround for HBASE-5711, we need to set config value
        // dfs.datanode.data.dir.perm
        // equal to the permissions of the temp dirs on the filesystem. These
        // temp dirs were
        // probably created using this process' umask. So we guess the temp dir
        // permissions as
        // 0777 & ~umask, and use that to set the config value.
        try {
            Process process = Runtime.getRuntime().exec("/bin/sh -c umask");
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            int rc = process.waitFor();
            if (rc == 0) {
                String umask = br.readLine();

                int umaskBits = Integer.parseInt(umask, 8);
                int permBits = 0777 & ~umaskBits;
                String perms = Integer.toString(permBits, 8);

                // log.info("Setting dfs.datanode.data.dir.perm to " + perms);

                jobConfig.set("dfs.datanode.data.dir.perm", perms);
            } else {
                throw new IOException("Failed running umask command in a shell, nonzero return value");
            }
        } catch (Exception e) {
            // ignore errors, we might not be running on POSIX, or "sh" might
            // not be on the path
            throw new IOException("Couldn't get umask", e);
        }

        dfsCluster = new MiniDFSCluster(jobConfig, dataNodes, true, null);
        fileSystem = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(0, 0, taskTrackers, fileSystem.getUri().toString(), 1, null, null, null,
                jobConfig);
    }

    /**
     * Destroys the MiniDFSCluster and MiniMRCluster Hadoop instance.
     *
     * @throws Exception
     *             if shutdown fails
     */
    public void close() throws Exception {
        mrCluster.shutdown();
        dfsCluster.shutdown();
    }

    /**
     * Returns the Filesystem.
     *
     * @return the filesystem used by Hadoop.
     */
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * Returns a job configuration preconfigured to run against the Hadoop
     * managed by this instance.
     *
     * @return configuration that works on the testcase Hadoop instance
     */
    public JobConf createJobConf() {
        return mrCluster.createJobConf();
    }

    /**
     * Returns a job configuration preconfigured to run against the Hadoop
     * managed by this instance.
     *
     * @param config
     *            a {@link JobConf} whose configuration is folded into the
     *            returned {@link JobConf}.
     * @return configuration that works on the testcase Hadoop instance
     */
    public JobConf createJobConf(final JobConf config) {
        return mrCluster.createJobConf(config);
    }
}
