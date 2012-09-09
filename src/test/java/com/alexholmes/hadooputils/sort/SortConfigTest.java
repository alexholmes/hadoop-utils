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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class SortConfigTest {

    private SortConfig config;

    @Before
    public void before() {
        config = new SortConfig(new Configuration());
    }

    @Test
    public void testIgnoreCase() throws IOException {
        assertFalse(config.getIgnoreCase());
        assertTrue(config.setIgnoreCase(true).getIgnoreCase());
        assertFalse(config.setIgnoreCase(false).getIgnoreCase());
    }

    @Test
    public void testUnique() throws IOException {
        assertFalse(config.getUnique());
        assertTrue(config.setUnique(true).getUnique());
        assertFalse(config.setUnique(false).getUnique());
    }

    @Test
    public void testStartKey() throws IOException {
        assertNull(config.getStartKey());
        assertEquals(new Integer(4), config.setStartKey(4).getStartKey());
    }

    @Test
    public void testEndKey() throws IOException {
        assertNull(config.getEndKey());
        assertEquals(new Integer(5), config.setEndKey(5).getEndKey());
    }

    @Test
    public void testFieldSeparator() throws IOException {
        assertEquals(" ", config.getFieldSeparator(" "));
        assertEquals("-", config.setFieldSeparator("-").getFieldSeparator(" "));
    }
}
