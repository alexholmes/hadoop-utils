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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

public class DelimitedTextOutputFormat<K, V> extends TextOutputFormat<K, V> {

    protected static class DelimitedLineRecordWriter<K, V> extends LineRecordWriter<K, V> {

        private static final String utf8 = "UTF-8";
        private final byte[] lineSeparator;
        private final byte[] keyValueSeparator;

        public DelimitedLineRecordWriter(DataOutputStream out, 
                                         String lineSeparator, 
                                         String keyValueSeparator) {
            super(out);
            try {
                this.lineSeparator = lineSeparator.getBytes(utf8);
                this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8
                    + " encoding");
            }
        }

        public DelimitedLineRecordWriter(DataOutputStream out, String lineSeparator) {
            this(out, lineSeparator, "\t");
        }

        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text) o;
                out.write(to.getBytes(), 0, to.getLength());
            } else {
                out.write(o.toString().getBytes(utf8));
            }
        }

        public synchronized void write(K key, V value)
            throws IOException {

            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                out.write(keyValueSeparator);
            }
            if (!nullValue) {
                writeObject(value);
            }
            out.write(lineSeparator);
        }

        public synchronized void close(Reporter reporter) throws IOException {
            out.close();
        }
    }

    public RecordWriter<K, V> getRecordWriter(FileSystem ignored,
                                              JobConf job,
                                              String name,
                                              Progressable progress)
        throws IOException {

        SortConfig sortConf = new SortConfig(job);
        boolean isCompressed = getCompressOutput(job);
        String lineSeparator = sortConf.getRowSeparator("\n");
        byte[] hexcode = SortConfig.getHexDelimiter(lineSeparator);
        lineSeparator = (hexcode != null) ? new String(hexcode, "UTF-8") : lineSeparator;

        if (!isCompressed) {
            Path file = FileOutputFormat.getTaskOutputPath(job, name);
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new DelimitedLineRecordWriter<K, V>(fileOut, lineSeparator);
        } else {
            Class<? extends CompressionCodec> codecClass =
                getOutputCompressorClass(job, GzipCodec.class);
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
            Path file = 
                FileOutputFormat.getTaskOutputPath(job, 
                                           name + codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new DelimitedLineRecordWriter<K, V>(new DataOutputStream
                                        (codec.createOutputStream(fileOut)),
                                        lineSeparator);
        }
    }
}
