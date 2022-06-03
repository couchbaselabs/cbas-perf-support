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

import os
import shutil


# Finds all the files under the provided path and returns
# the json files only. The returned output will be a tuple
# of json files with data, and json files that are empty
def getFiles(dirPath, extension):
    jsonFiles = []
    emptyFiles = []

    files = os.listdir(dirPath)
    for file in files:
        if file.endswith(extension):
            if os.stat(dirPath + file).st_size > 0:
                jsonFiles.append(file)
            else:
                emptyFiles.append(file)

    return jsonFiles, emptyFiles


# Recursively deletes all contents of the provided directory path
def cleanDir(dirPath):
    try:
        for fileName in os.listdir(dirPath):
            filePath = os.path.join(dirPath, fileName)

            if os.path.isfile(filePath) or os.path.islink(filePath):
                os.unlink(filePath)
            elif os.path.isdir(filePath):
                shutil.rmtree(filePath)
    except Exception as e:
        print('Failed to delete %s. Reason: %s' % (dirPath, e))


# Deletes the directory and its content
def deleteDir(dirPath):
    try:
        shutil.rmtree(dirPath)
    except Exception as e:
        print('Failed to delete %s. Reason: %s' % (dirPath, e))
