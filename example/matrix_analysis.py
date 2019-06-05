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
from pyspark.mllib.linalg.distributed import CoordinateMatrix
from pyspark.mllib.linalg.distributed import BlockMatrix

import scipy.io as sio

def do_spark(matrix):
    spark = SparkSession\
        .builder\
        .appName("MatrixAnalysis")\
        .config("spark.executor.memory", "2G")\
        .getOrCreate()

    dense_matrix = Matrices.dense(4875, 4875, matrix.toarray().flatten())
    rdd = spark.sparkContext.parallelize([((0, 0), dense_matrix), ((0, 1), dense_matrix)])
    block_matrix = BlockMatrix(rdd, 1024, 1024)
    print(block_matrix.add(block_matrix).toLocalMatrix())

    spark.stop()

if __name__ == "__main__":
    with open("crystm01.mtx") as file:
        matrix = sio.mmread(file)
        do_spark(matrix)