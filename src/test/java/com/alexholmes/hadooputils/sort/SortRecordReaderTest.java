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

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SortRecordReaderTest {

    @Test
    public void testDefaultExtractKey() throws IOException {
        assertEquals("asd", SortRecordReader.extractKey(new Text("asd"),
                null, // start key
                null, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("asd def", SortRecordReader.extractKey(new Text("asd def"),
                null, // start key
                null, // end key
                " ", // separator
                false // ignore case
        ).toString());
    }

    @Test
    public void testStartKey() throws IOException {
        assertEquals("asd", SortRecordReader.extractKey(new Text("asd"),
                1, // start key
                null, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("asd def", SortRecordReader.extractKey(new Text("asd def"),
                1, // start key
                null, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("asd def feg", SortRecordReader.extractKey(new Text("asd def feg"),
                1, // start key
                null, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("def", SortRecordReader.extractKey(new Text("asd def"),
                2, // start key
                null, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("def feg", SortRecordReader.extractKey(new Text("asd def feg"),
                2, // start key
                null, // end key
                " ", // separator
                false // ignore case
        ).toString());
    }

    @Test(expected = IOException.class)
    public void testStartKeyOutOfBounds() throws IOException {
        SortRecordReader.extractKey(new Text("asd"),
                2, // start key
                null, // end key
                " ", // separator
                false // ignore case
        );
    }

    @Test
    public void testStartEndKeys() throws IOException {
        assertEquals("asd", SortRecordReader.extractKey(new Text("asd"),
                1, // start key
                1, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("asd def", SortRecordReader.extractKey(new Text("asd def"),
                1, // start key
                2, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("asd def feg", SortRecordReader.extractKey(new Text("asd def feg"),
                1, // start key
                3, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("def", SortRecordReader.extractKey(new Text("asd def"),
                2, // start key
                2, // end key
                " ", // separator
                false // ignore case
        ).toString());

        assertEquals("def feg", SortRecordReader.extractKey(new Text("asd def feg"),
                2, // start key
                3, // end key
                " ", // separator
                false // ignore case
        ).toString());
    }

    @Test(expected = IOException.class)
    public void testEndKeyOutOfBounds() throws IOException {
        SortRecordReader.extractKey(new Text("asd"),
                1, // start key
                2, // end key
                " ", // separator
                false // ignore case
        );
    }

    @Test
    public void testSeparator() throws IOException {
        assertEquals("asd~def", SortRecordReader.extractKey(new Text("asd~def"),
                1, // start key
                2, // end key
                "~", // separator
                false // ignore case
        ).toString());

        assertEquals("asd", SortRecordReader.extractKey(new Text("asd~def"),
                1, // start key
                1, // end key
                "~", // separator
                false // ignore case
        ).toString());
    }

    @Test
    public void testIgnoreCase() throws IOException {
        assertEquals("aaabbb", SortRecordReader.extractKey(new Text("aaaBBB"),
                1, // start key
                1, // end key
                "~", // separator
                true // ignore case
        ).toString());
    }
}
