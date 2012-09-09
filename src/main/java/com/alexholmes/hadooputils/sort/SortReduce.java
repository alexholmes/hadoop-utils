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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * A reducer which can produce unique key output if configured to do so.
 */
public class SortReduce extends MapReduceBase
        implements Reducer<Text, Text, Text, NullWritable> {

    /**
     * The sort config.
     */
    private SortConfig sortConfig;

    @Override
    public void configure(final JobConf job) {
        super.configure(job);
        sortConfig = new SortConfig(job);
    }

    @Override
    public void reduce(final Text key, final Iterator<Text> values,
                       final OutputCollector<Text, NullWritable> output, final Reporter reporter)
            throws IOException {
        while (values.hasNext()) {
            output.collect(values.next(), NullWritable.get());
            if (sortConfig.getUnique()) {
                break;
            }
        }
    }
}
