/*
 * Copyright 2013 Alex Holmes
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

package com.alexholmes.hadooputils.combine.seqfile.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * This is a simple map-only job that uses the {@link CombineSequenceFileInputFormat} in an identity
 * job.
 */
public class CombineSequenceFileJob extends Configured implements Tool {

    /**
     * Usage string.
     */
    private static final String[] USAGE = {
            "bin/hadoop jar hadoop-utils-<version>.jar " + CombineSequenceFileJob.class.getName()
                    + "[OPTION]... INPUT_DIR OUTPUT_DIR",
    };

    /**
     * Print the usage.
     *
     * @return the Java exit code
     */
    static int printUsage() {
        System.out.println(StringUtils.join(USAGE, "\n"));
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    /**
     * The driver for program which works with command-line arguments.
     *
     * @param args command-line arguments
     * @return 0 if everything went well, non-zero for everything else
     * @throws Exception When there is communication problems with the
     *                   job tracker.
     */
    @SuppressWarnings("unchecked")
    public int run(final String[] args) throws Exception {

        if (args.length != 2) {
            return printUsage();
        }

        if (runJob(getConf(), args[0], args[1])) {
            return 0;
        }
        return 1;
    }

    /**
     * The driver for the MapReduce job.
     *
     * @param conf           configuration
     * @param inputDirAsString  input directory in CSV-form
     * @param outputDirAsString output directory
     * @return true if the job completed successfully
     * @throws java.io.IOException         if something went wrong
     * @throws java.net.URISyntaxException if a URI wasn't correctly formed
     */
    public boolean runJob(final Configuration conf, final String inputDirAsString, final String outputDirAsString)
            throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        Job job = new Job(conf);

        job.setJarByClass(CombineSequenceFileJob.class);
        job.setJobName("seqfilecombiner");

        job.setNumReduceTasks(0);

//        job.setMapperClass(IdentityMapper.class);

        job.setInputFormatClass(CombineSequenceFileInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputDirAsString);
        FileOutputFormat.setOutputPath(job, new Path(outputDirAsString));

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        boolean jobResult = job.waitForCompletion(true);
        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took "
                + TimeUnit.MILLISECONDS.toSeconds(endTime.getTime() - startTime.getTime())
                + " seconds.");

        return jobResult;
    }

    /**
     * Main entry point for the utility.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CombineSequenceFileJob(), args);
        System.exit(res);
    }
}
