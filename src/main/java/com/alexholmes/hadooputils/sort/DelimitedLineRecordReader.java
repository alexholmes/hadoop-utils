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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Reads a record from a text file. Treats keys as offset in file
 * and value as record. Supports user-defined record delimiters
 */
public class DelimitedLineRecordReader implements RecordReader<LongWritable, Text> {

    protected CompressionCodecFactory compressionCodecs = null;
    protected long start;
    protected long pos;
    protected long end;
    protected DelimitedLineReader in;
    protected int maxLineLength;
    protected FSDataInputStream fileIn;

    public DelimitedLineRecordReader(Configuration conf, FileSplit split) 
        throws IOException {
        initialize(conf, split);
    }

    protected void initialize(Configuration job, FileSplit split) throws IOException {
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        String rowDelim = job.get("textinputformat.record.delimiter", null);    
        if (codec != null) {
            if (rowDelim != null) {
                byte[] hexcode = SortConfig.getHexDelimiter(rowDelim);
                in = new DelimitedLineReader(codec.createInputStream(fileIn), job, 
                        (hexcode != null) ? hexcode : rowDelim.getBytes());
            } else {
                in = new DelimitedLineReader(codec.createInputStream(fileIn), job);
            }
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            if (rowDelim != null) {
                byte[] hexcode = SortConfig.getHexDelimiter(rowDelim);
                in = new DelimitedLineReader(fileIn, job, 
                        (hexcode != null) ? hexcode : rowDelim.getBytes());
            } else {
                in = new DelimitedLineReader(fileIn, job);
            }
        }
        if (skipFirstLine) {  // skip first line and re-establish "start".
              start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    /**
    * Get the progress within the split.
    */
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    /** Read a line. */
    public synchronized boolean next(LongWritable key, Text value)
        throws IOException {
  
        while (pos < end) {
            key.set(pos);

            int newSize = in.readLine(value, maxLineLength,
                                Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                         maxLineLength));
            if (newSize == 0) {
                return false;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                return true;
            }
        }

        return false;
    }
}
