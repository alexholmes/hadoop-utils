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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

/**
 * A {@link RecordReader} that works with {@link CombineFileSplit}'s generated via
 * {@link CombineSequenceFileInputFormat}. All this class really does is coordinate creation of
 * {@link SequenceFileRecordReader}'s for each split contained within the {@link CombineFileSplit}.
 *
 * @param <K> The type of the key in the SequenceFile.
 * @param <V> The type of the value in the SequenceFile.
 */
public class CombineSequenceFileRecordReader<K, V> implements RecordReader<K, V> {
    private static final Log LOG = LogFactory.getLog(CombineSequenceFileRecordReader.class);

    protected Configuration conf;
    protected int currentSplit = -1;
    protected SequenceFileRecordReader<K, V> reader;
    protected CombineFileSplit split;
    private long totalBytes;

    /**
     * Create an instance of the class.
     *
     * @param conf  the Hadoop config
     * @param split the input split
     * @throws IOException on io error
     */
    public CombineSequenceFileRecordReader(Configuration conf, CombineFileSplit split) throws IOException {
        this.conf = conf;

        this.split = split;

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
     * @throws java.io.IOException if we hit io errors
     */
    public boolean nextReader() throws IOException {
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

        reader = new SequenceFileRecordReader<K, V>(conf, fileSplit);
        return true;
    }

    /**
     * Return the progress within the input split
     *
     * @return 0.0 to 1.0 of the input byte range
     */
    @Override
    public float getProgress() throws IOException {
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
    public boolean next(K key, V value) throws IOException {

        while (reader != null) {
            if (reader.next(key, value)) {
                return true;
            }
            nextReader();
        }
        return false;
    }

    @Override
    public K createKey() {
        return reader.createKey();
    }

    @Override
    public V createValue() {
        return reader.createValue();
    }

    @Override
    public long getPos() throws IOException {
        return (long) (((float) totalBytes) * getProgress());
    }

    @Override
    public synchronized void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
