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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * A record reader which extracts the sort key, and the entire sort line as the key/value pair.
 */
public class SortRecordReader implements RecordReader<Text, Text> {

    /**
     * The wrapped {@link RecordReader} used to do the heavy lifting.
     */
    private final RecordReader<LongWritable, Text> reader;

    /**
     * The key used by the {@link LineRecordReader}.
     */
    private final LongWritable lineRecordReaderKey = new LongWritable();

    /**
     * The value used by the {@link LineRecordReader}.
     */
    private final Text lineRecordReaderValue = new Text();

    /**
     * The sort config.
     */
    private final SortConfig sortConfig;

    /**
     * Constructor.
     *
     * @param job    the job configuration
     * @param reader the record reader
     * @throws IOException if something goes wrong
     */
    public SortRecordReader(final JobConf job, final RecordReader<LongWritable, Text> reader)
            throws IOException {
        this.reader = reader;
        sortConfig = new SortConfig(job);
    }

    @Override
    public boolean next(final Text key, final Text value) throws IOException {

        boolean result = reader.next(lineRecordReaderKey, lineRecordReaderValue);

        if (!result) {
            return false;
        }

        key.set(extractKey(lineRecordReaderValue,
                sortConfig.getStartKey(),
                sortConfig.getEndKey(),
                sortConfig.getFieldSeparator(null),
                sortConfig.getIgnoreCase()));
        value.set(lineRecordReaderValue);

        return true;
    }

    /**
     * Extract the key from the sort line, using the spupplied options.
     *
     * @param value          the sort line
     * @param startKey       the start key, or null if there isn't one
     * @param endKey         the end key, or null if there isn't one
     * @param fieldSeparator the field separator, used if a start (and optionally end) key are set
     * @param ignoreCase     whether the result should be lower-cased to ensure case is ignored
     * @return the key
     * @throws IOException if something goes wrong
     */
    protected static Text extractKey(final Text value, final Integer startKey,
                                     final Integer endKey, final String fieldSeparator,
                                     final boolean ignoreCase) throws IOException {

        Text result = new Text();

        if (startKey == null) {
            result.set(value);
        } else {

            // startKey is 1-based in the Linux sort, so decrement them to be 0-based
            //
            int startIdx = startKey - 1;

            String[] parts = StringUtils.split(value.toString(), fieldSeparator);

            if (startIdx >= parts.length) {
                throw new IOException("Start index is greater than parts in line");
            }

            int endIdx = parts.length;

            if (endKey != null) {
                // endKey is also 1-based in the Linux sort, but the StringUtils.join
                // end index is exclusive, so no need to decrement
                //
                endIdx = endKey;
                if (endIdx > parts.length) {
                    throw new IOException("End index is greater than parts in line");
                }
            }

            result.set(StringUtils.join(parts, fieldSeparator, startIdx, endIdx));
        }

        if (ignoreCase) {
            result.set(result.toString().toLowerCase());
        }

        return result;
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return reader.getPos();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }
}
