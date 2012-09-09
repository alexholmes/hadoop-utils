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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * The {@link InputFormat} used for reading the source files. The key is the sort key, and the value
 * is the entire sort line.
 */
public class SortInputFormatOld extends FileInputFormat<Text, Text>
        implements JobConfigurable {

    /**
     * Compression codec factory.
     */
    private CompressionCodecFactory compressionCodecs = null;

    @Override
    public void configure(final JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    @Override
    protected boolean isSplitable(final FileSystem fs, final Path file) {
        return compressionCodecs.getCodec(file) == null;
    }

    @Override
    public RecordReader<Text, Text> getRecordReader(
            final InputSplit genericSplit, final JobConf job,
            final Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new SortRecordReader(job, new LineRecordReader(job, (FileSplit) genericSplit));
    }
}
