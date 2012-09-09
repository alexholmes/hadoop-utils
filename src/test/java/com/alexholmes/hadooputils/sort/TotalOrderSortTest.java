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

import com.alexholmes.hadooputils.test.MiniHadoopTestCase;
import com.alexholmes.hadooputils.test.TextIOJobBuilder;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.junit.Test;

public class TotalOrderSortTest extends MiniHadoopTestCase {

    @Test
    public void test() throws Exception {

        InputSampler.RandomSampler sampler = new InputSampler.RandomSampler(1.0, 6, 1);

        JobConf jobConf = super.getMiniHadoop().createJobConf();

        TextIOJobBuilder builder = new TextIOJobBuilder(
                super.getMiniHadoop().getFileSystem())
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("foo-hump")
                .addInput("clump-bar")
                .addExpectedOutput("clump-bar")
                .addExpectedOutput("foo-hump")
                .writeInputs();

        new SortConfig(jobConf).setUnique(true);

        SortTest.run(
                jobConf,
                builder,
                2,
                2,
                sampler);
    }
}
