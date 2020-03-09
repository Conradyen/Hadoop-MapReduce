#!/bin/sh

Q1_JAR_PATH=~/Documents/CS6350_Big_data/assignments/assignment1/out/artifacts/assignment1_jar/assignment1.jar
Q2_JAR_PATH=~/Documents/CS6350_Big_data/assignments/assignment1/Q2/out/artifacts/Q2_jar/Q2.jar
Q3_JAR_PATH=~/Documents/CS6350_Big_data/assignments/assignment1/Q3/out/artifacts/Q3_jar/Q3.jar
Q4_JAR_PATH=~/Documents/CS6350_Big_data/assignments/assignment1/Q4/out/artifacts/Q4_jar/Q4.jar

DATA_INPUT_1=/user/assignment1/soc-Livejornal1adj.txt
DATA_INPUT_2=/user/assignment1/userdata.txt

Q1_OUT_PATH=/user/assignment1/userdataout

Q2_OUT_PATH_1=/user/assignment1/temp
Q2_OUT_PATH_2=/user/assignment1/Q2out

Q3_OUT_PATH_1=/user/assignment1/Q3temp
Q3_OUT_PATH_2=/user/assignment1/Q3out

Q4_OUT_PATH_1=/user/assignment1/Q4temp
Q4_OUT_PATH_2=/user/assignment1/Q4out

./hadoop jar ${Q1_JAR_PATH} $DATA_INPUT_1 $Q1_OUT_PATH

./hadoop jar $Q2_JAR_PATH $DATA_INPUT_1 $Q2_OUT_PATH_1 $Q2_OUT_PATH_2

./hadoop jar $Q3_JAR_PATH $DATA_INPUT_1 $DATA_INPUT_2 $Q3_OUT_PATH_1 $Q3_OUT_PATH_2

./hadoop jar $Q4_JAR_PATH $DATA_INPUT_1 $DATA_INPUT_2 $Q4_OUT_PATH_1 $Q4_OUT_PATH_2
