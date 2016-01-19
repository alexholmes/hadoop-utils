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

import com.alexholmes.hadooputils.TestBase;
import com.alexholmes.hadooputils.test.TextIOJobBuilder;
import com.alexholmes.hadooputils.test.TextIOLocalJobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SortTest extends TestBase {

    public static void run(JobConf jobConf, TextIOJobBuilder builder, int numMapTasks, int numReduceTasks, InputSampler.Sampler sampler) throws Exception {

        Sort sort = new Sort();

        assertTrue(sort.runJob(
                jobConf, // job config
                numMapTasks, // num map tasks
                numReduceTasks, // num reduce tasks
                sampler, // sampler
                null, // job compression codec
                null, // map compression codec
                false, // LZOP index
                builder.getInputPath().toUri().getPath(), // input path
                builder.getOutputPath().toUri().getPath() // outputpath
        ));

        builder.verifyResults();
    }

    public void run(TextIOJobBuilder builder) throws Exception {
        run(new JobConf(new SortConfig(builder.getFs().getConf()).getConfig()), builder, 1, 1, null);
    }

    public void run(SortConfig sortConfig, TextIOJobBuilder builder) throws Exception {
        run(new JobConf(sortConfig.getConfig()), builder, 1, 1, null);
    }

    public void run(JobConf jobConf, TextIOJobBuilder builder) throws Exception {
        run(jobConf, builder, 1, 1, null);
    }

    @Test
    public void runSimple() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foobar")
                .addInput("abcdef")
                .addExpectedOutput("abcdef")
                .addExpectedOutput("foobar")
                .writeInputs();

        run(builder);
    }

    @Test
    public void runCaseSensitive() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foobar")
                .addInput("GHIdef")
                .addExpectedOutput("GHIdef")
                .addExpectedOutput("foobar")
                .writeInputs();

        run(builder);
    }

    @Test
    public void runIgnoreCase() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foobar")
                .addInput("GHIdef")
                .addExpectedOutput("foobar")
                .addExpectedOutput("GHIdef")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setIgnoreCase(true), builder);
    }

    @Test
    public void runNonUnique() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foobar")
                .addInput("foobar")
                .addExpectedOutput("foobar")
                .addExpectedOutput("foobar")
                .writeInputs();

        run(builder);
    }

    @Test
    public void runUnique() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foobar")
                .addInput("foobar")
                .addExpectedOutput("foobar")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setUnique(true), builder);
    }

    @Test
    public void runStartScenario1() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foo bar")
                .addInput("clump")
                .addExpectedOutput("clump")
                .addExpectedOutput("foo bar")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setStartKey(1), builder);
    }

    @Test
    public void runStartScenario2() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("abc bar")
                .addInput("clump baa")
                .addExpectedOutput("clump baa")
                .addExpectedOutput("abc bar")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setStartKey(2), builder);
    }

    @Test
    public void runEndScenario1() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foo bar")
                .addInput("clump")
                .addExpectedOutput("clump")
                .addExpectedOutput("foo bar")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setStartKey(1).setEndKey(1), builder);
    }

    @Test
    public void runEndScenario2() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foo bar")
                .addInput("clump hump")
                .addExpectedOutput("clump hump")
                .addExpectedOutput("foo bar")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setStartKey(1).setEndKey(2), builder);
    }

    @Test
    public void runEndScenario3() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foo bar")
                .addInput("clump hump")
                .addExpectedOutput("foo bar")
                .addExpectedOutput("clump hump")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setStartKey(2).setEndKey(2), builder);
    }

    @Test
    public void runFieldSeparator() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("foo-hump")
                .addInput("clump-bar")
                .addExpectedOutput("clump-bar")
                .addExpectedOutput("foo-hump")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setStartKey(2).setEndKey(2).setFieldSeparator("-"), builder);
    }

    @Test
    public void runNumeric() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("10 foobar")
                .addInput("2 foobar")
                .addExpectedOutput("2 foobar")
                .addExpectedOutput("10 foobar")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setNumeric(true).setStartKey(1).setEndKey(1).setFieldSeparator(" "), builder);
    }

    @Test
    public void runNumericComposite() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("10 10 10 foobar")
                .addInput("10 10 9 foobar")
                .addExpectedOutput("10 10 9 foobar")
                .addExpectedOutput("10 10 10 foobar")
                .writeInputs();

        run(new SortConfig(builder.getFs().getConf()).setNumeric(true).setStartKey(1).setEndKey(3).setFieldSeparator(" "), builder);
    }

    @Test
    public void runRowSeparator() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("10 10 10 foobar|10 10 9 foobar|")
                .addExpectedOutput("10 10 9 foobar|10 10 10 foobar|")
                .writeDelimitedInputs("");

        run(new SortConfig(builder.getFs().getConf()).setNumeric(true).setStartKey(1).setEndKey(3).setFieldSeparator(" ").setRowSeparator("|"), builder);
    }

    @Test
    public void runRowMultiSeparator() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput("10~~10~~10~~foobar@|@10~~10~~9~~foobar@|@")
                .addExpectedOutput("10~~10~~9~~foobar@|@10~~10~~10~~foobar@|@")
                .writeDelimitedInputs("");

        run(new SortConfig(builder.getFs().getConf()).setNumeric(true).setStartKey(3).setEndKey(3).setFieldSeparator("~~").setRowSeparator("@|@"), builder);
    }

    @Test
    public void runHexSeparator() throws Exception {

        TextIOJobBuilder builder = new TextIOLocalJobBuilder(new Configuration(), TEST_ROOT_DIR)
                .addInput(
                    "100"+Character.toString((char) 1)+
                    "bar"+Character.toString((char) 0)+
                    "10"+Character.toString((char) 1)+
                     "me"+Character.toString((char) 0))
                .addExpectedOutput(
                    "10"+Character.toString((char) 1)+
                     "me"+Character.toString((char) 0)+
                    "100"+Character.toString((char) 1)+
                    "bar"+Character.toString((char) 0))
                .writeDelimitedInputs("");

        run(new SortConfig(builder.getFs().getConf()).setStartKey(1).setEndKey(1).setFieldSeparator("0x01").setRowSeparator("0x00"), builder);
    }
}
