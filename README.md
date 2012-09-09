Hadoop Utils
============

A set of Hadoop utilities to make working with Hadoop a little easier. The utilities can be
called from your Hadoop application code, and in addition some can be called directly from the
command-line.

## License

Apache licensed (see contents of file `LICENSE`).

## Usage

### For integrating into your application code

To get started, simply:

1. Download, and run `mvn package`
2. Add `target/hadoop-utils-<version>.jar` into your application's.
3. Understand the API's by browsing the javadocs in `target/hadoop-utils-<version>-javadoc.jar`

### To run the bundled utilities

To get started, simply:

1. Download, and run `mvn package`
2. Copy the generated tarball under `target/` to a machine that has access to Hadoop, and untar.
3. Execute `hadoop jar hadoop-utils-<version>-jar-with-dependencies.jar <class>` with the approriate
classname.
4. Look at file `CLI.md` for how to run the utilities.
