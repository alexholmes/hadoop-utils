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

package com.alexholmes.hadooputils.combine.common.mapreduce;

import com.alexholmes.hadooputils.util.HadoopCompat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * A {@link RecordReader} that works with {@link CombineFileSplit}'s generated via
 * {@link org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat}.
 * <p/>
 * All this class really does is coordinate creation of
 * {@link RecordReader}'s for each split contained within the
 * {@link org.apache.hadoop.mapreduce.lib.input.CombineFileSplit}.
 *
 * @param <K> The type of the key in the SequenceFile.
 * @param <V> The type of the value in the SequenceFile.
 */
public class CommonCombineFileRecordReader<K, V> extends RecordReader {
    private static final Log LOG = LogFactory.getLog(CommonCombineFileRecordReader.class);

    protected Configuration conf;
    protected int currentSplit = -1;
    protected RecordReader<K, V> reader;
    protected CombineFileSplit split;
    private TaskAttemptContext context;
    private long totalBytes;
    private final RecordReaderEngineerer<K, V> engineerer;

    /**
     * Ctor.
     *
     * @param engineerer the engineerer that will create {@link RecordReader} instances
     */
    public CommonCombineFileRecordReader(RecordReaderEngineerer<K, V> engineerer) {
        this.engineerer = engineerer;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.context = context;

        this.split = (CombineFileSplit) split;
        conf = HadoopCompat.getConfiguration(context);

        for (int i = 0; i < this.split.getPaths().length; i++) {
            totalBytes += this.split.getLength(i);

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Got split %s offest %d len %d", this.split.getPath(i), this.split.getOffset(i), this.split.getLength(i)));
            }
        }

        nextReader();
    }

    /**
     * Moves on to the next split inside {@link #split}. The {@link #reader} will be {@code null}
     * once we have exhausted all the splits.
     *
     * @return true if we successfully moved on to the next split
     * @throws java.io.IOException  if we hit io errors
     * @throws InterruptedException if we get interrupted
     */
    public boolean nextReader() throws IOException, InterruptedException {
        // close the current reader and set it to null
        close();

        currentSplit++;

        if (currentSplit >= split.getPaths().length) {
            // hit the end of the line
            return false;
        }

        FileSplit fileSplit = new FileSplit(
                split.getPath(currentSplit),
                split.getOffset(currentSplit),
                split.getLength(currentSplit),
                split.getLocations() == null || split.getLocations().length - 1 < currentSplit ? null : new String[]{split.getLocations()[currentSplit]});

        reader = engineerer.createRecordReader();
        reader.initialize(fileSplit, context);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader != null) {
            if (reader.nextKeyValue()) {
                return true;
            }
            nextReader();
        }
        return false;
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return reader.getCurrentValue();
    }

    /**
     * Return the progress within the input split
     *
     * @return 0.0 to 1.0 of the input byte range
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (reader == null) {
            return 1.0f;
        }

        long completeBytes = 0;

        for (int i = 0; i < currentSplit - 1; i++) {
            completeBytes += split.getLength(i);
        }

        float currentReaderProgress = reader.getProgress();

        completeBytes += Math.min(split.getLength(currentSplit), (long) ((float) split.getLength(currentSplit)) * currentReaderProgress);

        return (float) completeBytes / (float) totalBytes;
    }

    @Override
    public synchronized void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    /**
     * Create {@link RecordReader} instances.
     *
     * @param <K> the {@link RecordReader} key type
     * @param <V> the {@link RecordReader} value type
     */
    public static interface RecordReaderEngineerer<K, V> {
        /**
         * Create an instance of a {@link RecordReader}.
         *
         * @return the RR
         */
        public RecordReader<K, V> createRecordReader();
    }
}
