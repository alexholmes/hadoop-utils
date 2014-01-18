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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * A record reader which extracts the sort key, and the entire sort line as the key/value pair.
 */
public class SortRecordReader implements RecordReader<ArrayWritable, Text> {

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
    public boolean next(final ArrayWritable key, final Text value) throws IOException {

        boolean result = reader.next(lineRecordReaderKey, lineRecordReaderValue);

        if (!result) {
            return false;
        }

        key.set(extractKey(lineRecordReaderValue,
                sortConfig.getStartKey(),
                sortConfig.getEndKey(),
                sortConfig.getFieldSeparator(null),
                sortConfig.getIgnoreCase(),
                sortConfig.getNumeric()));
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
     * @param isNumeric      whether the sort keys should be compared numerically
     * @return the key
     * @throws IOException if something goes wrong
     */
    protected static Writable[] extractKey(final Text value, final Integer startKey,
                                    final Integer endKey, final String fieldSeparator,
                                    final boolean ignoreCase, final boolean isNumeric) 
            throws IOException {

        Writable[] result = null;

        if (startKey == null) {
            if (isNumeric) {
                throw new IOException("Sort key must be specified");
            } else {
                result = new Text[1];
                if (ignoreCase) {
                    result[0] = new Text(value.toString().toLowerCase());
                } else {
                    result[0] = new Text(value);
                }
            }
        } else {

            // startKey is 1-based in the Linux sort, so decrement them to be 0-based
            //
            int startIdx = startKey - 1;

            byte[] hexcode = SortConfig.getHexDelimiter(fieldSeparator);
            String[] parts = StringUtils.splitByWholeSeparatorPreserveAllTokens(
                value.toString(), (hexcode != null) ? new String(hexcode, "UTF-8") : fieldSeparator);

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

            if (isNumeric) {
                result = new LongWritable[endIdx - startIdx];
                for (int i = startIdx; i < endIdx; i++) {
                        result[i - startIdx] = new LongWritable(Long.parseLong(parts[i]));
                } 
            } else {
                result = new Text[endIdx - startIdx];
                for (int i = startIdx; i < endIdx; i++) {
                    if (ignoreCase) {
                        result[i - startIdx] = new Text(parts[i].toLowerCase());
                    } else {
                        result[i - startIdx] = new Text(parts[i]);
                    }
                }
            }
        }

        return result;
    }

    @Override
    public ArrayWritable createKey() {
        if (sortConfig.getNumeric()) {
            return new LongArrayWritable();
        } else {
            return new TextArrayWritable();
        }
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
