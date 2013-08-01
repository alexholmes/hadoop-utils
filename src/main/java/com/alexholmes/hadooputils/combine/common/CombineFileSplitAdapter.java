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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

/**
 */
public class CombineFileSplitAdapter {
    org.apache.hadoop.mapred.lib.CombineFileSplit mapredSplit;
    org.apache.hadoop.mapreduce.lib.input.CombineFileSplit mapreduceSplit;

    public CombineFileSplitAdapter(CombineFileSplit mapredSplit) {
        this.mapredSplit = mapredSplit;
    }

    public CombineFileSplitAdapter(org.apache.hadoop.mapreduce.lib.input.CombineFileSplit mapreduceSplit) {
        this.mapreduceSplit = mapreduceSplit;
    }

    public boolean isMapRedSet() {
        return mapredSplit != null;
    }

    public long getLength() {
        return isMapRedSet() ? mapredSplit.getLength() : mapreduceSplit.getLength();
    }

    /** Returns an array containing the start offsets of the files in the split*/
    public long[] getStartOffsets() {
        return isMapRedSet() ? mapredSplit.getStartOffsets() : mapreduceSplit.getStartOffsets();
    }

    /** Returns an array containing the lengths of the files in the split*/
    public long[] getLengths() {
        return isMapRedSet() ? mapredSplit.getLengths() : mapreduceSplit.getLengths();
    }

    /** Returns the start offset of the i<sup>th</sup> Path */
    public long getOffset(int i) {
        return isMapRedSet() ? mapredSplit.getOffset(i) : mapreduceSplit.getOffset(i);
    }

    /** Returns the length of the i<sup>th</sup> Path */
    public long getLength(int i) {
        return isMapRedSet() ? mapredSplit.getLength(i) : mapreduceSplit.getLength(i);
    }

    /** Returns the number of Paths in the split */
    public int getNumPaths() {
        return isMapRedSet() ? mapredSplit.getNumPaths() : mapreduceSplit.getNumPaths();
    }

    /** Returns the i<sup>th</sup> Path */
    public Path getPath(int i) {
        return isMapRedSet() ? mapredSplit.getPath(i) : mapreduceSplit.getPath(i);
    }

    /** Returns all the Paths in the split */
    public Path[] getPaths() {
        return isMapRedSet() ? mapredSplit.getPaths() : mapreduceSplit.getPaths();
    }

    /** Returns all the Paths where this input-split resides */
    public String[] getLocations() throws IOException {
        return isMapRedSet() ? mapredSplit.getLocations() : mapreduceSplit.getLocations();
    }
}
