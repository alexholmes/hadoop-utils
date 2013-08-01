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

import com.alexholmes.hadooputils.combine.common.CombineFileSplitAdapter;
import com.alexholmes.hadooputils.combine.common.LoggerSink;
import com.alexholmes.hadooputils.combine.common.MetricsSink;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An input format that writes split details to a sink, the default sink being a logger.
 *
 * @param <K> The key type.
 * @param <V> The value type.
 */
public abstract class SplitMetricsCombineInputFormat<K, V> extends CombineFileInputFormat<K, V> {
    private static final Log LOG = LogFactory.getLog(SplitMetricsCombineInputFormat.class);

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = super.getSplits(job);

        if (job.getConfiguration().getBoolean("hadooputils.combine.sink.enabled", false)) {
            writeSplitsToSink(job.getConfiguration(), organizeSplitsByLocation(splits));
        }

        return splits;
    }

    protected void writeSplitsToSink(Configuration conf, Map<String, List<CombineFileSplitAdapter>> splits) {

        Class<? extends MetricsSink> theClass = conf.getClass("hadooputils.combine.sink.class", LoggerSink.class, MetricsSink.class);

        MetricsSink sink = ReflectionUtils.newInstance(theClass, conf);

        for (Map.Entry<String, List<CombineFileSplitAdapter>> entry : splits.entrySet()) {
            sink.pushLocation(entry.getKey(), entry.getValue());
        }
    }

    public static Map<String, List<CombineFileSplitAdapter>> organizeSplitsByLocation(final List<InputSplit> splits) throws IOException {
        Map<String, List<CombineFileSplitAdapter>> locationSplits = new HashMap<String, List<CombineFileSplitAdapter>>();

        for (InputSplit split : splits) {
            CombineFileSplit csplit = (CombineFileSplit) split;

            String location = extractLocation(csplit);

            List<CombineFileSplitAdapter> splitsForLocation = locationSplits.get(location);
            if (splitsForLocation == null) {
                splitsForLocation = new ArrayList<CombineFileSplitAdapter>();
                locationSplits.put(location, splitsForLocation);
            }
            splitsForLocation.add(new CombineFileSplitAdapter(csplit));
        }
        return locationSplits;
    }

    public static String extractLocation(CombineFileSplit split) throws IOException {
        if (split.getLocations() == null || split.getLocations().length == 0) {
            return null;
        }
        return split.getLocations()[0];
    }
}
