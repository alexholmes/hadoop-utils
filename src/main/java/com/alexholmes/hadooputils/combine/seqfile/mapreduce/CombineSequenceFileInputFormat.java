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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} which can feed multiple
 * SequenceFiles input splits to individual mappers. This is useful in situations where you have a large
 * number of SequenceFiles that are at or smaller than the HDFS block size, and you wish to have a sensible
 * cap on the number of reducers that run.
 *
 * @param <K> The type of the key in the SequenceFile.
 * @param <V> The type of the value in the SequenceFile.
 */
public class CombineSequenceFileInputFormat<K, V> extends CombineFileInputFormat {
    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineSequenceFileRecordReader<K, V>();
    }

    @Override
    protected long getFormatMinSplitSize() {
        return SequenceFile.SYNC_INTERVAL;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<FileStatus> listStatus(JobContext job) throws IOException {

        List<FileStatus> files = super.listStatus(job);

        int len = files.size();
        for (int i = 0; i < len; ++i) {
            FileStatus file = files.get(i);
            if (file.isDir()) {     // it's a MapFile
                Path p = file.getPath();
                FileSystem fs = p.getFileSystem(job.getConfiguration());
                // use the data file
                files.set(i, fs.getFileStatus(new Path(p, MapFile.DATA_FILE_NAME)));
            }
        }
        return files;
    }
}
