/*
 * Copyright 2012 Alex Holmes
 * Modified work Copyright 2014 Mark Cusack
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

import com.hadoop.compression.lzo.LzoInputFormatCommon;
import com.hadoop.mapred.DeprecatedLzoTextInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * The {@link org.apache.hadoop.mapred.InputFormat} used for reading the source files. The key
 * is the sort key, and the value is the entire sort line.
 */
public class SortInputFormat extends DeprecatedLzoTextInputFormat
        implements JobConfigurable {

    @Override
    public void configure(final JobConf conf) {
        super.configure(conf);

        // by default the DeprecatedLzoTextInputFormat.listStatus will ignore
        // files that don't end in ".lzo". since we want to work with any file
        // we turn this feature off
        //
        conf.setBoolean(LzoInputFormatCommon.IGNORE_NONLZO_KEY, false);
    }

    @Override
    protected boolean isSplitable(final FileSystem fs, final Path file) {
        return super.isSplitable(fs, file);
    }

    @Override
    public RecordReader getRecordReader(
            final InputSplit split, final JobConf conf, 
            final Reporter reporter) 
            throws IOException {
        FileSplit fileSplit = (FileSplit) split;
        if (LzoInputFormatCommon.isLzoFile(fileSplit.getPath().toString())) {
            reporter.setStatus(split.toString());
            return new SortRecordReader(conf, new LzoDelimitedLineRecordReader(conf, fileSplit));
        } else {
            return new SortRecordReader(conf, new DelimitedLineRecordReader(conf, fileSplit));
        }
    }
}
