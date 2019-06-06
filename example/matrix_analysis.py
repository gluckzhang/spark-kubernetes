#!/usr/bin/python
# -*- coding: utf-8 -*-
# Filename: matrix_analysis.py

from __future__ import print_function

import sys, getopt, logging

from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Matrices
from pyspark.mllib.linalg.distributed import MatrixEntry
from pyspark.mllib.linalg.distributed import CoordinateMatrix
from pyspark.mllib.linalg.distributed import BlockMatrix

import scipy.io as sio

MATRIX_FILE_PATH = ''

def main():
    handle_args(sys.argv[1:])
    with open(MATRIX_FILE_PATH) as file:
        matrix = sio.mmread(file)
        do_spark(matrix)

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
    transposed_matrix = block_matrix.transpose()
    logging.info(transposed_matrix.toLocalMatrix())

    spark.stop()

def handle_args(argv):
    global MATRIX_FILE_PATH

    try:
        opts, args = getopt.getopt(argv, "f:", ["file=", "help"])
    except getopt.GetoptError as error:
        logging.error(error)
        print_help_info()
        sys.exit(2)

    for opt, arg in opts:
        if opt == "--help":
            print_help_info()
            sys.exit()
        elif opt in ("-f", "--file"):
            MATRIX_FILE_PATH = arg

    if MATRIX_FILE_PATH == '':
        logging.error("You should use -f or --file to specify the path to your matrix")
        print_help_info()
        sys.exit(2)

def print_help_info():
    print('')
    print('MatrixAnalysis Tool Help Info')
    print('    matrix_analysis.py -f <matrix_file_path>')
    print('or: matrix_analysis.py --file=<matrix_file_path>')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()