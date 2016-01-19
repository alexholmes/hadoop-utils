/*
 * Copyright 2014 Mark Cusack
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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class LongArrayWritable extends ArrayWritable 
    implements WritableComparable<LongArrayWritable> {

    public LongArrayWritable() { 
        super(LongWritable.class); 
    } 

    public int compareTo(LongArrayWritable o) {
        Writable[] theseLongs = get();
        Writable[] thoseLongs = o.get();
        int len = Math.min(theseLongs.length, thoseLongs.length);
        for (int i = 0; i < len; i++) {
            LongWritable thisLong = (LongWritable) theseLongs[i];
            LongWritable thatLong = (LongWritable) thoseLongs[i];
            int comp = thisLong.compareTo(thatLong);
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        String[] theseStrings = toStrings();
        for (int i = 0; i < theseStrings.length ; i++) {
            result = result * prime + theseStrings[i].hashCode();
        }
        return result;
    }
}

