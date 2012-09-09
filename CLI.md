Hadoop Utils CLI
================

A set of Hadoop utilities to make working with Hadoop a little easier.

# Basic setup

1. Download, and run `mvn package`
2. Copy the generated tarball `target/hadoop-utils-<version>-package.tar.gz` to a machine that has
access to Hadoop, and untar it.
3. Look at the following sections for specific instructions on running the utilities.

# Sorting files

The `com.alexholmes.hadooputils.sort.Sort` class provides a MapReduce job to sort files with a
syntax similar to Linux's sort. To view usage execute:

<pre><code>shell$ hadoop jar hadoop-utils-<version>-jar-with-dependencies.jar com.alexholmes.hadooputils.sort.Sort
ERROR: Wrong number of parameters: 0 instead of 2.
bin/hadoop jar hadoop-utils-<version>.jar com.alexholmes.hadooputils.sort.Sort [OPTION]... INPUT_DIR OUTPUT_DIR

Ordering options:
-f, --ignore-case
       Fold lower case to upper case characters.

Other options:

-m MAPS
       The number of map tasks.
-r REDUCERS
       The number of reduce tasks.
-k, --key POS1[,POS2]
       Start a key at POS1 (origin 1), end it at POS2 (default end of line).
-t, --field-separator SEP
       Use SEP instead of non-blank to blank transition.
-u, --unique
       Output only the first of an equal run.
--total-order PCNT NUM_SAMPLES MAX_SPLITS
       Produce total order across all reducer files.
         PCNT = Probability with which a key will be chosen (range 0.0 - 1.0).
         NUM_SAMPLES = Number of samples which will be extracted.
         MAX_SPLITS = Number of input splits to extract samples from.
--map-codec CODEC
       Compression codec for map intermediary outputs.
--codec CODEC
       Compression codec for final outputs.
--lzop-index
       Creates LZOP indexes for the output files.
</code></pre>

First copy the bundled test file into HDFS

<pre><code>shell$ hadoop fs -put test-data/300names.txt .
</code></pre>

To sort and write the sorted output in LZOP-compressed format (and create LZOP indexes!):

<pre><code>shell$ hadoop jar hadoop-utils-<version>-jar-with-dependencies.jar com.alexholmes.hadooputils.sort.Sort \
        -r 2 --total-order 0.1 10000 10 --codec com.hadoop.compression.lzo.LzopCodec --lzopIndex \
        300names.txt 300names-sorted
shell$ hadoop fs -ls 300names-sorted
Found 6 items
-rw-r--r--   1 aholmes supergroup          0 2012-09-08 21:20 /user/aholmes/300names-sorted/_SUCCESS
drwxr-xr-x   - aholmes supergroup          0 2012-09-08 21:20 /user/aholmes/300names-sorted/_logs
-rw-r--r--   1 aholmes supergroup       2039 2012-09-08 21:20 /user/aholmes/300names-sorted/part-00000.lzo
-rw-r--r--   1 aholmes supergroup          8 2012-09-08 21:20 /user/aholmes/300names-sorted/part-00000.lzo.index
-rw-r--r--   1 aholmes supergroup       1548 2012-09-08 21:20 /user/aholmes/300names-sorted/part-00001.lzo
-rw-r--r--   1 aholmes supergroup          8 2012-09-08 21:20 /user/aholmes/300names-sorted/part-00001.lzo.index
</code></pre>
