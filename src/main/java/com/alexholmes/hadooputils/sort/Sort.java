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

package com.alexholmes.hadooputils.sort;

import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This is a simple MapReduce sorting utility, modeled after the Linux sort utility.
 * It supports a subset of the options available with Linux sort.
 */
public class Sort<K, V> extends Configured implements Tool {

    /**
     * Details about the job.
     */
    private RunningJob jobResult = null;

    /**
     * Usage string.
     */
    private static final String[] USAGE = {
            "bin/hadoop jar hadoop-utils-<version>.jar com.alexholmes.hadooputils.sort.Sort "
                    + "[OPTION]... INPUT_DIR OUTPUT_DIR",
            "",
            "Ordering options:",
            "-f, --ignore-case",
            "         Fold lower case to upper case characters.",
            "",
            "Other options:",
            "",
            "-m MAPS",
            "         The number of map tasks.",
            "-r REDUCERS",
            "         The number of reduce tasks.",
            "-k, --key POS1[,POS2]",
            "         Start a key at POS1 (origin 1), end it at POS2 (default end of line).",
            "-t, --field-separator SEP",
            "         Use SEP instead of non-blank to blank transition.",
            "-u, --unique",
            "         Output only the first of an equal run.",
            "--totalOrder PCNT NUM_SAMPLES MAX_SPLITS",
            "         Produce total order across all reducer files.",
            "           PCNT = Probability with which a key will be chosen (range 0.0 - 1.0).",
            "           NUM_SAMPLES = Number of samples which will be extracted.",
            "           MAX_SPLITS = Number of input splits to extract samples from.",
            "--mapCodec CODEC",
            "         Compression codec for map intermediary outputs.",
            "--codec CODEC",
            "         Compression codec for final outputs.",
            "--lzopIndex",
            "         Creates LZOP indexes for the output files.",
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
     * The driver for sort program which works with command-line arguments.
     *
     * @param args command-line arguments
     * @return 0 if everything went well, non-zero for everything else
     * @throws Exception When there is communication problems with the
     *                   job tracker.
     */
    @SuppressWarnings("unchecked")
    public int run(final String[] args) throws Exception {

        SortConfig sortConfig = new SortConfig(getConf());

        Integer numMapTasks = null;
        Integer numReduceTasks = null;

        List<String> otherArgs = new ArrayList<String>();
        InputSampler.Sampler<K, V> sampler = null;
        Class<? extends CompressionCodec> codecClass = null;
        Class<? extends CompressionCodec> mapCodecClass = null;
        boolean createLzopIndex = false;
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    numMapTasks = Integer.parseInt(args[++i]);
                } else if ("-r".equals(args[i])) {
                    numReduceTasks = Integer.parseInt(args[++i]);
                } else if ("-f".equals(args[i]) || "--ignore-case".equals(args[i])) {
                    sortConfig.setIgnoreCase(true);
                } else if ("-u".equals(args[i]) || "--unique".equals(args[i])) {
                    sortConfig.setUnique(true);
                } else if ("-k".equals(args[i]) || "--key".equals(args[i])) {
                    String[] parts = StringUtils.split(args[++i], ",");
                    sortConfig.setStartKey(Integer.valueOf(parts[0]));
                    if (parts.length > 1) {
                        sortConfig.setEndKey(Integer.valueOf(parts[1]));
                    }
                } else if ("-t".equals(args[i]) || "--field-separator".equals(args[i])) {
                    sortConfig.setFieldSeparator(args[++i]);
                } else if ("--totalOrder".equals(args[i])) {
                    double pcnt = Double.parseDouble(args[++i]);
                    int numSamples = Integer.parseInt(args[++i]);
                    int maxSplits = Integer.parseInt(args[++i]);
                    if (0 >= maxSplits) {
                        maxSplits = Integer.MAX_VALUE;
                    }
                    sampler = new InputSampler.RandomSampler<K, V>(pcnt, numSamples, maxSplits);
                } else if ("--mapCodec".equals(args[i])) {
                    mapCodecClass = (Class<? extends CompressionCodec>) Class.forName(args[++i]);
                } else if ("--codec".equals(args[i])) {
                    codecClass = (Class<? extends CompressionCodec>) Class.forName(args[++i]);
                } else if ("--codec".equals(args[i])) {
                    codecClass = (Class<? extends CompressionCodec>) Class.forName(args[++i]);
                } else if ("--lzopIndex".equals(args[i])) {
                    createLzopIndex = true;
                } else {
                    otherArgs.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from "
                        + args[i - 1]);
                return printUsage(); // exits
            }
        }

        // Make sure there are exactly 2 parameters left.
        if (otherArgs.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: "
                    + otherArgs.size() + " instead of 2.");
            return printUsage();
        }

        if (runJob(new JobConf(sortConfig.getConfig()), numMapTasks, numReduceTasks, sampler,
                codecClass, mapCodecClass, createLzopIndex, otherArgs.get(0), otherArgs.get(1))) {
            return 0;
        }
        return 1;
    }

    /**
     * The driver for the sort MapReduce job.
     *
     * @param jobConf           sort configuration
     * @param numMapTasks       number of map tasks
     * @param numReduceTasks    number of reduce tasks
     * @param sampler           sampler, if required
     * @param codecClass        the compression codec for compressing final outputs
     * @param mapCodecClass     the compression codec for compressing intermediary map outputs
     * @param createLzopIndexes whether or not a MR job should be launched to create LZOP indexes
     *                          for the job output files
     * @param inputDirAsString  input directory in CSV-form
     * @param outputDirAsString output directory
     * @return true if the job completed successfully
     * @throws IOException        if something went wrong
     * @throws URISyntaxException if a URI wasn't correctly formed
     */
    public boolean runJob(final JobConf jobConf, final Integer numMapTasks,
                          final Integer numReduceTasks, final InputSampler.Sampler<K, V> sampler,
                          final Class<? extends CompressionCodec> codecClass,
                          final Class<? extends CompressionCodec> mapCodecClass,
                          final boolean createLzopIndexes,
                          final String inputDirAsString, final String outputDirAsString)
            throws IOException, URISyntaxException {

        jobConf.setJarByClass(Sort.class);
        jobConf.setJobName("sorter");

        JobClient client = new JobClient(jobConf);
        ClusterStatus cluster = client.getClusterStatus();

        if (numMapTasks != null) {
            jobConf.setNumMapTasks(numMapTasks);
        }
        if (numReduceTasks != null) {
            jobConf.setNumReduceTasks(numReduceTasks);
        } else {
            int numReduces = (int) (cluster.getMaxReduceTasks() * 0.9);
            String sortReduces = jobConf.get("test.sort.reduces_per_host");
            if (sortReduces != null) {
                numReduces = cluster.getTaskTrackers() * Integer.parseInt(sortReduces);
            }

            // Set user-supplied (possibly default) job configs
            jobConf.setNumReduceTasks(numReduces);
        }

        jobConf.setMapperClass(IdentityMapper.class);
        jobConf.setReducerClass(SortReduce.class);

        jobConf.setInputFormat(SortInputFormat.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        if (mapCodecClass != null) {
            jobConf.setMapOutputCompressorClass(mapCodecClass);
        }

        if (codecClass != null) {
            jobConf.setBoolean("mapred.output.compress", true);
            jobConf.setClass("mapred.output.compression.codec",
                    codecClass, CompressionCodec.class);
        }

        FileInputFormat.setInputPaths(jobConf, inputDirAsString);
        FileOutputFormat.setOutputPath(jobConf, new Path(outputDirAsString));

        if (sampler != null) {
            System.out.println("Sampling input to effect total-order sort...");
            jobConf.setPartitionerClass(TotalOrderPartitioner.class);
            Path inputDir = FileInputFormat.getInputPaths(jobConf)[0];

            FileSystem fileSystem = FileSystem.get(jobConf);

            if (fileSystem.exists(inputDir) && fileSystem.isFile(inputDir)) {
                inputDir = inputDir.getParent();
            }
            inputDir = inputDir.makeQualified(inputDir.getFileSystem(jobConf));
            Path partitionFile = new Path(inputDir, "_sortPartitioning");
            TotalOrderPartitioner.setPartitionFile(jobConf, partitionFile);
            InputSampler.writePartitionFile(jobConf, sampler);
            URI partitionUri = new URI(partitionFile.toString()
                    + "#" + "_sortPartitioning");
            DistributedCache.addCacheFile(partitionUri, jobConf);
            DistributedCache.createSymlink(jobConf);
        }

        System.out.println("Running on "
                + cluster.getTaskTrackers()
                + " nodes to sort from "
                + FileInputFormat.getInputPaths(jobConf)[0] + " into "
                + FileOutputFormat.getOutputPath(jobConf)
                + " with " + jobConf.getNumReduceTasks() + " reduces.");
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        jobResult = JobClient.runJob(jobConf);
        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took "
                + TimeUnit.MILLISECONDS.toSeconds(endTime.getTime() - startTime.getTime())
                + " seconds.");

        if (jobResult.isSuccessful()) {
            if (createLzopIndexes && codecClass != null && LzopCodec.class.equals(codecClass)) {
                new LzoIndexer(jobConf).index(new Path(outputDirAsString));
            }
            return true;
        }
        return false;
    }

    /**
     * Main entry point for the utility.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Sort(), args);
        System.exit(res);
    }

    /**
     * Get the last job that was run using this instance.
     *
     * @return the results of the last job that was run
     */
    public RunningJob getResult() {
        return jobResult;
    }
}
