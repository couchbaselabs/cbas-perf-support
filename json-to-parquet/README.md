# Introduction
This tool can be used to convert JSON files (JSON record-per-line objects) into parquet files. The tool
uses Spark under the hood to convert the data to parquet.

# Requirements
- Java 8 or 11 (required by Spark, Java versions newer than 11 are not supported by Spark yet)
- Scala (required by Spark, although Spark 3+ comes pre-built with Scala, so this step might not be necessary)
- Spark 3+

# Instructions
- Git clone `https://github.com/couchbaselabs/cbas-perf-support`
- Install Java 8 or 11 and make sure JAVA_HOME environment variable is set (read `Important` section in `How To Use`
if you don't want to change your `JAVA_HOME` environment variable).
- Install Scala.
- Install Spark 3+ and make sure SPARK_HOME environment variable is set.

# How To Use
## Important
- When running the tool, the content of the `data/output` folder will be deleted to clean up
the output folder, so the output of previous runs needs to be copied if desired before subsequent runs.
- If you don't want to change your `JAVA_HOME` environment variable, you can set the `JAVA_HOME`
in the `$SPARK_HOME/conf/spark-env.sh`, this will take precedence over the JAVA_HOME
in environment variable. You can add `export "JAVA_HOME=<path-to-your-java-home>"` in
the `spark-env.sh` to set the `JAVA_HOME` for spark only.

Once the setup is complete, the tool can be used as follows:
- Navigate to `<your-cloned-project>/json-to-parquet`
- Put the JSON files to-be converted to parquet in the `data/input` folder.
- Run the tool with the following command `spark-submit main.py` (`spark-submit` is available in `$SPARK_HOME/bin`)
- The generated files will be in the `data/output` folder. The converted files will have the same name with
the extension showing as `.parquet` instead of `.json`

# Limitations
- JSON field names should not contain a whitespace (e.g. `{"my field": "someValue"}`) as whitespace is not
allowed in field names for parquet.
- The tool only supports single level in the `data/input` folder, the tool does not support subfolders
structure, so make sure all the JSON files are on the top level folder.
- The JSON files should contain `record-per-line` JSON objects.