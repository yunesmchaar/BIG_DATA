# Hadoop Cancer Diagnosis Analysis

## Description
This project analyzes a medical cancer dataset using Apache Hadoop and MapReduce.
The goal is to count the number of benign (B) and malignant (M) cancer cases.

## Technologies
- Apache Hadoop
- HDFS
- MapReduce (Java)
- Ubuntu Linux

## Dataset
Breast Cancer Dataset (CSV format)

## Results
- Benign (B): 357
- Malignant (M): 212

## How to run
bash
javac -classpath $(hadoop classpath) -d . CancerCount.java
jar cf wc.jar *.class
hdfs dfs -rm -r /cancer_output
hadoop jar wc.jar CancerCount
