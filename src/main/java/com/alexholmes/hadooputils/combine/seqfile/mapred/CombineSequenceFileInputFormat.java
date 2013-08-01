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

import com.alexholmes.hadooputils.combine.common.mapred.CommonCombineRecordReader;
import com.alexholmes.hadooputils.combine.common.mapred.SplitMetricsCombineInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

/**
 * An {@link org.apache.hadoop.mapred.InputFormat} which can feed multiple
 * SequenceFiles input splits to individual mappers. This is useful in situations where you have a large
 * number of SequenceFiles that are at or smaller than the HDFS block size, and you wish to have a sensible
 * cap on the number of reducers that run.
 *
 * Bear in mind that by default the {@link CombineFileInputFormat} will only create a single input split
 * for each node, which means only 1 mapper for the job will run on the node (under normal operating conditions).
 * Therefore the default behavior impacts the mapper parallelism. You can cap the maximum number of
 * bytes in an input split by either calling {@link CombineFileInputFormat#setMaxSplitSize(long)},
 * or by setting the configurable property {@code mapred.max.split.size}.
 *
 * @param <K> The type of the key in the SequenceFile.
 * @param <V> The type of the value in the SequenceFile.
 */
public class CombineSequenceFileInputFormat<K, V> extends SplitMetricsCombineInputFormat<K, V> {

    /**
     * Ctor.
     */
    public CombineSequenceFileInputFormat() {
        setMinSplitSize(SequenceFile.SYNC_INTERVAL);
    }

    @Override
    protected FileStatus[] listStatus(JobConf job) throws IOException {
        FileStatus[] files = super.listStatus(job);
        for (int i = 0; i < files.length; i++) {
            FileStatus file = files[i];
            if (file.isDir()) {     // it's a MapFile
                Path dataFile = new Path(file.getPath(), MapFile.DATA_FILE_NAME);
                FileSystem fs = file.getPath().getFileSystem(job);
                // use the data file
                files[i] = fs.getFileStatus(dataFile);
            }
        }
        return files;
    }

    @SuppressWarnings("unchecked")
    public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());

        return new CommonCombineRecordReader(job, (CombineFileSplit) split, new CommonCombineRecordReader.RecordReaderEngineerer() {
            @Override
            public RecordReader createRecordReader(Configuration conf, FileSplit split) throws IOException {
                return new SequenceFileRecordReader<K, V>(conf, split);
            }
        });
    }
}

