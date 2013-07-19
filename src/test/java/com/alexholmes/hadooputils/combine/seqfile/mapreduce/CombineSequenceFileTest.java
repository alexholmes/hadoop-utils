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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class CombineSequenceFileTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static Text key = new Text("k1");
    private static Text value = new Text("v1");

    public void writeSequenceFile(Path path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer writer =
                SequenceFile.createWriter(fs, conf, path, Text.class,
                        Text.class,
                        SequenceFile.CompressionType.BLOCK,
                        new DefaultCodec());
        try {
            writer.append(key, value);
        } finally {
            writer.close();
        }
    }

    @Test
    public void testOneFile() throws IOException, InterruptedException {
        Path dir = new Path(tempFolder.getRoot().getAbsolutePath());

        CombineSequenceFileInputFormat<Text, Text> inputFormat = new CombineSequenceFileInputFormat<Text, Text>();
        Path inputFile = new Path(dir, "file1.txt");

        writeSequenceFile(inputFile);

        Job job = new Job(new JobConf());

        FileInputFormat.addInputPath(job, inputFile);

        List<InputSplit> splits = inputFormat.getSplits(job);
        assertEquals(1, splits.size());


        TaskAttemptID taskId = new TaskAttemptID("jt", 0, true, 0, 0);
        Configuration conf1 = new Configuration();
        TaskAttemptContext context1 = new TaskAttemptContext(conf1, taskId);

        RecordReader<Text, Text> rr = inputFormat.createRecordReader(splits.get(0), context1);
        rr.initialize(splits.get(0), context1);
        assertTrue(rr.nextKeyValue());

        assertEquals(key, rr.getCurrentKey());
        assertEquals(value, rr.getCurrentValue());

        assertFalse(rr.nextKeyValue());
        assertEquals(1.0f, rr.getProgress(), 0.1);
    }

    @Test
    public void testTwoFiles() throws IOException, InterruptedException {
        Path dir = new Path(tempFolder.getRoot().getAbsolutePath());

        CombineSequenceFileInputFormat<Text, Text> inputFormat = new CombineSequenceFileInputFormat<Text, Text>();
        Path inputFile1 = new Path(dir, "file1.txt");
        Path inputFile2 = new Path(dir, "file2.txt");

        writeSequenceFile(inputFile1);
        writeSequenceFile(inputFile2);

        Job job = new Job(new JobConf());

        FileInputFormat.addInputPath(job, inputFile1);
        FileInputFormat.addInputPath(job, inputFile2);

        List<InputSplit> splits = inputFormat.getSplits(job);
        assertEquals(1, splits.size());


        TaskAttemptID taskId = new TaskAttemptID("jt", 0, true, 0, 0);
        Configuration conf1 = new Configuration();
        TaskAttemptContext context1 = new TaskAttemptContext(conf1, taskId);

        RecordReader<Text, Text> rr = inputFormat.createRecordReader(splits.get(0), context1);
        rr.initialize(splits.get(0), context1);
        assertTrue(rr.nextKeyValue());

        assertEquals(key, rr.getCurrentKey());
        assertEquals(value, rr.getCurrentValue());

        assertEquals(0.5f, rr.getProgress(), 0.1);

        assertTrue(rr.nextKeyValue());

        assertEquals(key, rr.getCurrentKey());
        assertEquals(value, rr.getCurrentValue());

        assertFalse(rr.nextKeyValue());
        assertEquals(1.0f, rr.getProgress(), 0.1);
    }
}
