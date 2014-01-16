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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SortRecordReaderTest {

    @Test
    public void testDefaultExtractKey() throws IOException {
        Writable[] key = SortRecordReader.extractKey(new Text("asd"),
                null, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("asd", key[0].toString());

        key = SortRecordReader.extractKey(new Text("asd def"),
                null, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("asd def", key[0].toString());
    }

    @Test
    public void testStartKey() throws IOException {
        Writable[] key = SortRecordReader.extractKey(new Text("asd"),
                1, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("asd", key[0].toString());

        key = SortRecordReader.extractKey(new Text("asd def"),
                1, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(2, key.length);
        assertEquals("asd", key[0].toString());
        assertEquals("def", key[1].toString());

        key = SortRecordReader.extractKey(new Text("asd def feg"),
                1, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(3, key.length);
        assertEquals("asd", key[0].toString());
        assertEquals("def", key[1].toString());
        assertEquals("feg", key[2].toString());

        key = SortRecordReader.extractKey(new Text("asd def"),
                2, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("def", key[0].toString());

        key = SortRecordReader.extractKey(new Text("asd def feg"),
                2, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(2, key.length);
        assertEquals("def", key[0].toString());

        key = SortRecordReader.extractKey(new Text("asd def feg"),
                3, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("feg", key[0].toString());
    }

    @Test(expected = IOException.class)
    public void testStartKeyOutOfBounds() throws IOException {
        SortRecordReader.extractKey(new Text("asd"),
                2, // start key
                null, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
    }

    @Test
    public void testStartEndKeys() throws IOException {
        Writable[] key = SortRecordReader.extractKey(new Text("asd"),
                1, // start key
                1, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("asd", key[0].toString());

        key = SortRecordReader.extractKey(new Text("asd def"),
                1, // start key
                2, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(2, key.length);
        assertEquals("asd", key[0].toString());
        assertEquals("def", key[1].toString());

        key = SortRecordReader.extractKey(new Text("asd def feg"),
                1, // start key
                3, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(3, key.length);
        assertEquals("asd", key[0].toString());
        assertEquals("def", key[1].toString());
        assertEquals("feg", key[2].toString());

        key = SortRecordReader.extractKey(new Text("asd def"),
                2, // start key
                2, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("def", key[0].toString());

        key = SortRecordReader.extractKey(new Text("asd def feg"),
                2, // start key
                3, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(2, key.length);
        assertEquals("def", key[0].toString());
        assertEquals("feg", key[1].toString());
    }

    @Test(expected = IOException.class)
    public void testEndKeyOutOfBounds() throws IOException {
        SortRecordReader.extractKey(new Text("asd"),
                1, // start key
                2, // end key
                " ", // separator
                false, // ignore case
                false // is numeric key
        );
    }

    @Test
    public void testSeparator() throws IOException {
        Writable[] key = SortRecordReader.extractKey(new Text("asd~def"),
                1, // start key
                2, // end key
                "~", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(2, key.length);
        assertEquals("asd", key[0].toString());
        assertEquals("def", key[1].toString());
         
        key = SortRecordReader.extractKey(new Text("asd~def"),
                1, // start key
                1, // end key
                "~", // separator
                false, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("asd", key[0].toString());
    }

    @Test
    public void testIgnoreCase() throws IOException {
        Writable[] key = SortRecordReader.extractKey(new Text("aaaBBB"),
                1, // start key
                1, // end key
                "~", // separator
                true, // ignore case
                false // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertEquals("aaabbb", key[0].toString());
    }

    @Test
    public void testNumeric() throws IOException {
        Writable[] key = SortRecordReader.extractKey(new Text("1 2 3"),
                1, // start key
                1, // end key
                " ", // separator
                false, // ignore case
                true // is numeric key
        );
        assertNotNull(key);
        assertEquals(1, key.length);
        assertTrue(key[0] instanceof LongWritable);
        assertEquals(1, ((LongWritable)key[0]).get());
    }

}
