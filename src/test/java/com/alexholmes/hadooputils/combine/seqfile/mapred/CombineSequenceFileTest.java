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

package com.alexholmes.hadooputils.combine.seqfile.mapred;

import com.alexholmes.hadooputils.combine.common.mapred.CombineSequenceFileRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

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

        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf);

        FileInputFormat.addInputPath(jobConf, inputFile);

        InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
        assertEquals(1, splits.length);

        CombineSequenceFileRecordReader<Text, Text> rr = (CombineSequenceFileRecordReader<Text, Text>) inputFormat.getRecordReader(splits[0], jobConf, new DummyReporter());
        Text k = new Text();
        Text v = new Text();
        assertTrue(rr.next(k, v));

        assertEquals(key, k);
        assertEquals(value, v);

        assertFalse(rr.next(k, v));
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

        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf);

        FileInputFormat.addInputPath(jobConf, inputFile1);
        FileInputFormat.addInputPath(jobConf, inputFile2);

        InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
        assertEquals(1, splits.length);

        CombineSequenceFileRecordReader<Text, Text> rr = (CombineSequenceFileRecordReader<Text, Text>) inputFormat.getRecordReader(splits[0], jobConf, new DummyReporter());
        Text k = new Text();
        Text v = new Text();

        assertTrue(rr.next(k, v));

        assertEquals(key, k);
        assertEquals(value, v);

        assertEquals(0.5f, rr.getProgress(), 0.1);

        assertTrue(rr.next(k, v));

        assertEquals(key, k);
        assertEquals(value, v);

        assertFalse(rr.next(k, v));
        assertEquals(1.0f, rr.getProgress(), 0.1);
    }

    public static class DummyReporter implements Reporter {

        @Override
        public void setStatus(String status) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Counters.Counter getCounter(Enum<?> name) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Counters.Counter getCounter(String group, String name) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void incrCounter(Enum<?> key, long amount) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void incrCounter(String group, String counter, long amount) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public InputSplit getInputSplit() throws UnsupportedOperationException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public float getProgress() {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void progress() {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
