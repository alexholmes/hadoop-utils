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

package com.alexholmes.hadooputils.combine.seqfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generates a SequenceFile in a directory with a configurable number of records.
 */
public class SequenceFileGenerator extends Configured implements Tool {

    public int run(final String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(String.format("Usage: %s: <file path> <number of records>", SequenceFileGenerator.class.getName()));
            return 1;
        }

        Path file = new Path(args[0]);
        int numRecords = Integer.valueOf(args[1]);

        FileSystem fs = FileSystem.get(super.getConf());

        SequenceFile.Writer writer =
                SequenceFile.createWriter(fs, super.getConf(), file, Text.class,
                        Text.class,
                        SequenceFile.CompressionType.BLOCK,
                        new DefaultCodec());
        try {
            for (int i=0; i < numRecords; i++) {
                writer.append(new Text("k" + i), new Text("v" + i));
            }
        } finally {
            writer.close();
        }

        return 0;
    }

    /**
     * Main entry point for the utility.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SequenceFileGenerator(), args);
        System.exit(res);
    }
}
