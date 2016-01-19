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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;

/**
 * Reads a record from an lzo compressed text file. Treats keys as offset in file
 * and value as record. Supports user-defined record delimiters
 */
public class LzoDelimitedLineRecordReader extends DelimitedLineRecordReader {

    public LzoDelimitedLineRecordReader(Configuration conf, FileSplit split) 
        throws IOException {
        super(conf, split);
    }

    @Override
    protected void initialize(Configuration job, FileSplit split) throws IOException {
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
    
        FileSystem fs = file.getFileSystem(job);
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (codec == null) {
            throw new IOException("No codec for file " + file + " not found, cannot run");
        }

        // open the file and seek to the start of the split
        fileIn = fs.open(split.getPath());

        // creates input stream and also reads the file header
        String rowDelim = job.get("textinputformat.record.delimiter", null);    
        if (rowDelim != null) {
                byte[] hexcode = SortConfig.getHexDelimiter(rowDelim);
                in = new DelimitedLineReader(fileIn, job, 
                        (hexcode != null) ? hexcode : rowDelim.getBytes());
        } else {
            in = new DelimitedLineReader(codec.createInputStream(fileIn), job);
        }

        if (start != 0) {
            fileIn.seek(start);

            // read and ignore the first line
            in.readLine(new Text());
            start = fileIn.getPos();
        }

        this.pos = start;
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
        //since the lzop codec reads everything in lzo blocks
        //we can't stop if the pos == end
        //instead we wait for the next block to be read in when
        //pos will be > end
        while (pos <= end) {
          key.set(pos);

          int newSize = in.readLine(value);
          if (newSize == 0) {
            return false;
          }
          pos = fileIn.getPos();

          return true;
        }

        return false;
    }
}
