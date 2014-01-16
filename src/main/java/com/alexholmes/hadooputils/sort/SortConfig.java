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

import org.apache.hadoop.conf.Configuration;

/**
 * A convenience class which reads and writes sort configurations
 * from a wrapped {@link Configuration}.
 */
public class SortConfig {

    /**
     * The wrapped configuration object.
     */
    private final Configuration config;

    /**
     * Configuration for ignoring case when sorting.
     */
    private static final String IGNORE_CASE = "sort.ignorecase";

    /**
     * Configuration for only including unique values in the sort results.
     */
    private static final String UNIQUE = "sort.unique";

    /**
     * Configuration for numerically sorted results.
     */
    private static final String NUMERIC = "sort.numeric";

    /**
     * Configuration for the start key when sorting.
     */
    private static final String START_KEY = "sort.key.start";

    /**
     * Configuration for the end key when sorting.
     */
    private static final String END_KEY = "sort.key.end";

    /**
     * Configuration for the field separator when sorting.
     */
    private static final String FIELD_SEPARATOR = "sort.field.separator";

    /**
     * Configuration for the row separator when sorting.
     */
    private static final String ROW_SEPARATOR = "textinputformat.record.delimiter";

    /**
     * Configuration for the task timeout in seconds.
     */
    private static final String TASK_TIMEOUT = "mapred.task.timeout";

    /**
     * Constructor, which takes a {link Configuration} object.
     *
     * @param config the Hadoop configuration object
     */
    public SortConfig(final Configuration config) {
        this.config = config;
    }

    /**
     * Set whether case should be ignored when sorted.
     *
     * @param ignoreCase true if the case should be ignored for sorting
     * @return reference to this object
     */
    public SortConfig setIgnoreCase(final boolean ignoreCase) {
        config.setBoolean(IGNORE_CASE, ignoreCase);
        return this;
    }

    /**
     * Get whether case should be ignored when sorted.
     *
     * @return true if the case should be ignored for sorting
     */
    public boolean getIgnoreCase() {
        return config.getBoolean(IGNORE_CASE, false);
    }

    /**
     * Set whether only a single output should be included for duplicates.
     *
     * @param unique true if a single output should be included for duplicates
     * @return reference to this object
     */
    public SortConfig setUnique(final boolean unique) {
        config.setBoolean(UNIQUE, unique);
        return this;
    }

    /**
     * Get whether only a single output should be included for duplicates.
     *
     * @return true if a single output should be included for duplicates
     */
    public boolean getUnique() {
        return config.getBoolean(UNIQUE, false);
    }

    /**
     * Set whether records should be numerically sorted.
     *
     * @param numeric true if the output should be sorted numerically
     * @return reference to this object
     */
    public SortConfig setNumeric(final boolean numeric) {
        config.setBoolean(NUMERIC, numeric);
        return this;
    }

    /**
     * Get whether records should be numerically sorted.
     *
     * @return true if records should be numerically sorted
     */
    public boolean getNumeric() {
        return config.getBoolean(NUMERIC, false);
    }

    /**
     * Set the start index used for sorting.
     *
     * @param key a key containing the start index used for sorting
     * @return reference to this object
     */
    public SortConfig setStartKey(final int key) {
        config.set(START_KEY, String.valueOf(key));
        return this;
    }

    /**
     * Get the start index used for sorting.
     *
     * @return a key containing the start index used for sorting, or null if one wasn't set
     */
    public Integer getStartKey() {
        String val = config.get(START_KEY, null);
        if (val != null) {
            return Integer.valueOf(val);
        }
        return null;
    }

    /**
     * Set the end index used for sorting.
     *
     * @param key a key containing the end index used for sorting
     * @return reference to this object
     */
    public SortConfig setEndKey(final int key) {
        config.set(END_KEY, String.valueOf(key));
        return this;
    }

    /**
     * Get the end index used for sorting.
     *
     * @return a key containing the end index used for sorting, or null if one wasn't set
     */
    public Integer getEndKey() {
        String val = config.get(END_KEY, null);
        if (val != null) {
            return Integer.valueOf(val);
        }
        return null;
    }

    /**
     * Set the field separator.
     *
     * @param key the field separator
     * @return reference to this object
     */
    public SortConfig setFieldSeparator(final String key) {
        config.set(FIELD_SEPARATOR, key);
        return this;
    }

    /**
     * Get the field separator.
     *
     * @param defaultValue the default value which is returned if the field separator isn't set
     * @return the field separator
     */
    public String getFieldSeparator(final String defaultValue) {
        return config.get(FIELD_SEPARATOR, defaultValue);
    }

    /**
     * Set the row separator.
     *
     * @param key the row separator
     * @return reference to this object
     */
    public SortConfig setRowSeparator(final String key) {
        config.set(ROW_SEPARATOR, key);
        return this;
    }

    /**
     * Get the row separator.
     *
     * @param defaultValue the default value which is returned if the row separator isn't set
     * @return the row separator
     */
    public String getRowSeparator(final String defaultValue) {
        return config.get(ROW_SEPARATOR, defaultValue);
    }

    /**
     * Set the task timeout.
     *
     * @param key the timeout in seconds
     * @return reference to this object
     */
    public SortConfig setTaskTimeout(final Long key) {
        config.setLong(TASK_TIMEOUT, key);
        return this;
    }

    /**
     * Get the task timeout.
     *
     * @param defaultValue the default value which is returned if the timeout isn't set
     * @return the task timeout in seconds
     */
    public Long getTaskTimeout(final Long defaultValue) {
        return config.getLong(TASK_TIMEOUT, defaultValue);
    }

    /**
     * Get the wrapped configuration object.
     *
     * @return the config object
     */
    public Configuration getConfig() {
        return config;
    }
}
