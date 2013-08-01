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

package com.alexholmes.hadooputils.combine.avro.mapreduce;

import com.alexholmes.hadooputils.combine.common.mapreduce.CommonCombineFileRecordReader;
import com.alexholmes.hadooputils.combine.common.mapreduce.SplitMetricsCombineInputFormat;
import com.alexholmes.hadooputils.util.HadoopCompat;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} which can feed multiple
 * input splits for Avro container files of key/value generic records  to individual mappers. This is useful in situations where you have a large
 * number of Avro files that are at or smaller than the HDFS block size, and you wish to have a sensible
 * cap on the number of reducers that run.
 *
 * Bear in mind that by default the {@link CombineFileInputFormat} will only create a single input split
 * for each node, which means only 1 mapper for the job will run on the node (under normal operating conditions).
 * Therefore the default behavior impacts the mapper parallelism. You can cap the maximum number of
 * bytes in an input split by either calling {@link CombineFileInputFormat#setMaxSplitSize(long)},
 * or by setting the configurable property {@code mapreduce.input.fileinputformat.split.maxsize}.
 *
 * @param <K> The type of the key in the Avro file.
 * @param <V> The type of the value in the Avro file.
 */
public class CombineAvroKeyValueInputFormat<K, V> extends SplitMetricsCombineInputFormat<AvroKey<K>, AvroValue<V>> {
    private static final Logger LOG = LoggerFactory.getLogger(CombineAvroKeyValueInputFormat.class);

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public RecordReader<AvroKey<K>, AvroValue<V>> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        final Schema keyReaderSchema = AvroJob.getInputKeySchema(HadoopCompat.getConfiguration(context));
        if (null == keyReaderSchema) {
            LOG.warn("Key reader schema was not set. Use AvroJob.setInputKeySchema() if desired.");
            LOG.info("Using a key reader schema equal to the writer schema.");
        }
        final Schema valueReaderSchema = AvroJob.getInputValueSchema(HadoopCompat.getConfiguration(context));
        if (null == valueReaderSchema) {
            LOG.warn("Value reader schema was not set. Use AvroJob.setInputValueSchema() if desired.");
            LOG.info("Using a value reader schema equal to the writer schema.");
        }

        return new CommonCombineFileRecordReader<K, V>(new CommonCombineFileRecordReader.RecordReaderEngineerer<K, V>() {
            @Override
            public RecordReader createRecordReader() {
                return new AvroKeyValueRecordReader<K, V>(keyReaderSchema, valueReaderSchema);
            }
        });
    }
}