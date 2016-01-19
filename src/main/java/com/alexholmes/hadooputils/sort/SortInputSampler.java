/*
 * Copyright 2014 Mark Cusack
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 * Work around a bug in the mapred/mapreduce interface which can
 * lead to the default TextInputFormat being selected rather than
 * the InputFormat required to read the input.  The problem manifests
 * itself through a mismatch of keys. 
 * Mark Cusack 2014/01/19
 */

public class SortInputSampler<K,V> extends InputSampler<K, V> {

    public SortInputSampler(JobConf conf) {
        super(conf);
    }

    public static <K,V> void writePartitionFile(JobConf job, Sampler<K,V> sampler)
            throws IOException {
        Configuration conf = job;
        // Use the input format defined in the job. NOT, the one provided by
        // the parent class's writePartitionFile() method, which will be a plain
        // TextInputFormat, by default
        final InputFormat inf = job.getInputFormat();
        int numPartitions = job.getNumReduceTasks();
        K[] samples = (K[])sampler.getSample(inf, job);
        RawComparator<K> comparator =
            (RawComparator<K>) job.getOutputKeyComparator();
        Arrays.sort(samples, comparator);
        Path dst = new Path(TotalOrderPartitioner.getPartitionFile(job));
        FileSystem fs = dst.getFileSystem(conf);
        if (fs.exists(dst)) {
            fs.delete(dst, false);
        }
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, 
            conf, dst, job.getMapOutputKeyClass(), NullWritable.class);
        NullWritable nullValue = NullWritable.get();
        float stepSize = samples.length / (float) numPartitions;
        int last = -1;
        for(int i = 1; i < numPartitions; ++i) {
            int k = Math.round(stepSize * i);
            while (last >= k && comparator.compare(samples[last], samples[k]) == 0) {
                ++k;
            }
            writer.append(samples[k], nullValue);
            last = k;
        }
        writer.close();
    }
}
