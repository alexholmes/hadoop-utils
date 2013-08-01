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

package com.alexholmes.hadooputils.combine.avro.mapred;

import com.alexholmes.hadooputils.combine.common.mapred.CommonCombineRecordReader;
import com.alexholmes.hadooputils.combine.common.mapred.SplitMetricsCombineInputFormat;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link org.apache.hadoop.mapred.InputFormat} which can feed multiple
 * input splits for Avro data files to individual mappers. This is useful in situations where you have a large
 * number of Avro files that are at or smaller than the HDFS block size, and you wish to have a sensible
 * cap on the number of reducers that run.
 * <p/>
 * Bear in mind that by default the {@link org.apache.hadoop.mapred.lib.CombineFileInputFormat} will only create a single input split
 * for each node, which means only 1 mapper for the job will run on the node (under normal operating conditions).
 * Therefore the default behavior impacts the mapper parallelism. You can cap the maximum number of
 * bytes in an input split by either calling {@link org.apache.hadoop.mapred.lib.CombineFileInputFormat#setMaxSplitSize(long)},
 * or by setting the configurable property {@code mapred.max.split.size}.
 *
 * @param <T> The type of the record in the Avro file.
 */
public class CombineAvroInputFormat<T> extends SplitMetricsCombineInputFormat<AvroWrapper<T>, NullWritable> {

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        for (FileStatus file : super.listStatus(job))
            if (file.getPath().getName().endsWith(AvroOutputFormat.EXT))
                result.add(file);
        return result.toArray(new FileStatus[0]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public RecordReader<AvroWrapper<T>, NullWritable>
    getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter)
            throws IOException {
        reporter.setStatus(split.toString());

        return new CommonCombineRecordReader(job, (CombineFileSplit) split, new CommonCombineRecordReader.RecordReaderEngineerer() {
            @Override
            public RecordReader createRecordReader(Configuration conf, FileSplit split) throws IOException {
                return new AvroRecordReader<T>(job, (FileSplit) split);
            }
        });
    }
}
