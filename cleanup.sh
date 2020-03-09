
#!/bin/sh
 ./hdfs dfsadmin -safemode leave
 
COMMONF_OUT_PATH=[OUT_PATH]

TOP_F_OUT_PATH_1=[TEMP_OUT_PATH]
TOP_F_OUT_PATH_2=[OUT_PATH]

DOB_OUT_PATH_1=[TEMP_OUT_PATH]
DOB_OUT_PATH_2=[OUT_PATH]

MAX_OUT_PATH_1=[TEMP_OUT_PATH]
MAX_OUT_PATH_2=[OUT_PATH]

#Q1
echo "clean Q1 output ..."
# remove part-r-00000
./hdfs dfs -rm -r -f [COMMONF_OUT_PATH/part-r-00000]
# remove _SUCCESS
./hdfs dfs -rm -r -f [COMMONF_OUT_PATH/_SUCCESS]
# remove dir
./hdfs dfs -rmdir [COMMONF_OUT_PATH]
echo "Q1 output removed "


#Q2
echo "clean Q2 output ..."
  
# remove part-r-00000
./hdfs dfs -rm -r -f [TOP_F_OUT_PATH_1/part-r-00000]
# remove _SUCCESS
./hdfs dfs -rm -r -f [TOP_F_OUT_PATH_1/_SUCCESS]
# remove dir
./hdfs dfs -rmdir [TOP_F_OUT_PATH_1]
# remove part-r-00000
./hdfs dfs -rm -r -f [TOP_F_OUT_PATH_2/part-r-00000]
# remove _SUCCESS
./hdfs dfs -rm -r -f [TOP_F_OUT_PATH_2/_SUCCESS]
# remove dir
./hdfs dfs -rmdir [TOP_F_OUT_PATH_2]
echo "Q2 output removed "

#Q3
echo "clean Q3 output ..."
# remove part-r-00000
./hdfs dfs -rm -r -f [DOB_OUT_PATH_1/part-r-00000]
# remove _SUCCESS
./hdfs dfs -rm -r -f [DOB_OUT_PATH_1/_SUCCESS]
# remove dir
./hdfs dfs -rmdir [DOB_OUT_PATH_1]
# remove part-r-00000
./hdfs dfs -rm -r -f [DOB_OUT_PATH_2/part-r-00000]
# remove _SUCCESS
./hdfs dfs -rm -r -f [DOB_OUT_PATH_2/_SUCCESS]
# remove dir
./hdfs dfs -rmdir [DOB_OUT_PATH_2]
echo "Q3 output removed "


#Q4
echo "clean Q4 output ..."
# remove part-r-00000
./hdfs dfs -rm -r -f [MAX_OUT_PATH_1/part-r-00000]
# remove _SUCCESS
./hdfs dfs -rm -r -f [MAX_OUT_PATH_1/_SUCCESS]
# remove dir
./hdfs dfs -rmdir [MAX_OUT_PATH_1]
# remove part-r-00000
./hdfs dfs -rm -r -f [MAX_OUT_PATH_2/part-r-00000]
# remove _SUCCESS
./hdfs dfs -rm -r -f [MAX_OUT_PATH_2/_SUCCESS]
# remove dir
./hdfs dfs -rmdir [MAX_OUT_PATH_2]
echo "Q4 output removed "
