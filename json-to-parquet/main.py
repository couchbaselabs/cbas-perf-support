#
# Copyright (c) 2022 Couchbase, Inc All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import os
import re
import shutil

from pyspark.sql import SparkSession
import scripts.utils as utils

# statics
INPUT_PATH = "data/input/"
INT_OUTPUT_PATH = "data/int-output/"
OUTPUT_PATH = "data/output/"
INPUT_EXTENSION = ".json"
OUTPUT_EXTENSION = ".parquet"

# delete output directories
utils.deleteDir(INT_OUTPUT_PATH)
utils.deleteDir(OUTPUT_PATH)

# recreate the output directories
os.mkdir(INT_OUTPUT_PATH)
os.mkdir(OUTPUT_PATH)

# include only files ending with .json
jsonFiles, emptyFiles = utils.getFiles(INPUT_PATH, ".json")

# ensure there are files to convert
if len(jsonFiles) == 0:
    print("No JSON files to convert to Parquet, make sure the files are under %s" % INPUT_PATH)
    sys.exit()

# create spark session
# we are using local[1] to use 1 thread and generate a single part for now
spark = SparkSession.builder.master("local[1]").appName("JSON to Parquet").getOrCreate()

# start converting JSON files to Parquet
# spark will actually use the provided path to create a folder with that name and set the written parts to it
for file in jsonFiles:
    dataframe = spark.read.json(INPUT_PATH + file)
    dataframe.write.parquet(INT_OUTPUT_PATH + file[0:len(file) - len(INPUT_EXTENSION)])

# spark fails if a file is empty because it needs at least 1 row to infer the schema from it
# TODO(htowaileb): need to write a valid empty parquet file if input is empty, issue is, it requires a schema
# for file in emptyFiles:
#     dataframe = spark.read.json(inputPath + file)
#     dataframe.write.parquet(intOutputPath + file[0:len(file) - len(inputExtension)])

# spark writes the files prefixed with "part-" as it does things in, we will rename each file to its original
# so the json_file path is actually a directory path now

regex = re.compile(".*.parquet$")  # we only need the file ending with .parquet

for dirName in jsonFiles:
    # note: the file names got created as directories by spark
    dirName = dirName[0: len(dirName) - len(INPUT_EXTENSION)]  # remove the extension

    # file name we want to use
    fullDirPath = os.path.join(INT_OUTPUT_PATH, dirName)
    desiredFileName = os.path.join(OUTPUT_PATH, dirName) + OUTPUT_EXTENSION

    # When spark writes the output, the file name provided becomes a folder, and in it are the generated parts by spark.
    # The content includes the .parquet, the .parquet.crc and successful or failure and the .crc respective.
    # We only need the .parquet result, we will move it from int-output to output
    # Example
    # data/int-output/real-file-name/part-000.parquet
    # will be moved to:
    # data/output/real-file-name.parquet
    filesInDir = os.listdir(fullDirPath)
    generatedFileName = list(filter(regex.match, filesInDir))[0]
    shutil.move(os.path.join(INT_OUTPUT_PATH, dirName, generatedFileName), desiredFileName)
