#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Matrices
from pyspark.mllib.linalg.distributed import MatrixEntry
from pyspark.mllib.linalg.distributed import CoordinateMatrix
from pyspark.mllib.linalg.distributed import BlockMatrix

import scipy.io as sio

def do_spark(matrix):
    spark = SparkSession\
        .builder\
        .appName("MatrixAnalysis")\
        .config("spark.driver.memory", "1536M")\
        .config("spark.executor.memory", "1024M")\
        .config("spark.sql.shuffle.partitions", 1000)\
        .getOrCreate()

    entries = list()
    for i,j,v in zip(matrix.row, matrix.col, matrix.data):
        entries.append(MatrixEntry(i, j, v))

    rdd = spark.sparkContext.parallelize(entries)

    coo_matrix = CoordinateMatrix(rdd)
    block_matrix = coo_matrix.toBlockMatrix(1024, 1024)
    print(block_matrix.transpose().toLocalMatrix())

    spark.stop()

if __name__ == "__main__":
    with open("crystm01.mtx") as file:
        matrix = sio.mmread(file)
        do_spark(matrix)