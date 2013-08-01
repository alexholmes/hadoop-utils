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

package com.alexholmes.hadooputils.combine.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.util.List;

/**
 * Logs combine split details.
 */
public class LoggerSink implements MetricsSink {
    private static final Log LOG = LogFactory.getLog(LoggerSink.class);

    @Override
    public void pushLocation(String location, List<CombineFileSplitAdapter> splits) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Location: " + location);
            LOG.info("Number of combine splits: " + splits.size());
            LOG.info("Size of combine splits in bytes: " + calcTotalSplitSizes(splits));
            for (int x = 0; x < splits.size(); x++) {
                CombineFileSplitAdapter split = splits.get(x);
                LOG.info(String.format("Split %d (of %d) has total size in bytes: %d", x+1, splits.size(), calcTotalSplitSizes(split)));
                for (int i = 0; i < split.getNumPaths(); i++) {
                    LOG.info(String.format("  %s %d:+%d", split.getPath(i), split.getOffset(i), split.getLength(i)));
                }
            }
        }
    }

    public static long calcTotalSplitSizes(CombineFileSplitAdapter split) {
        long len = 0;
        for (int i = 0; i < split.getNumPaths(); i++) {
            len += split.getLength(i);
        }
        return len;
    }

    public static long calcTotalSplitSizes(List<CombineFileSplitAdapter> splits) {
        long len = 0;
        for (CombineFileSplitAdapter split : splits) {
            len += calcTotalSplitSizes(split);
        }
        return len;
    }
}
