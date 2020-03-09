
#!/bin/sh
 ./hdfs dfsadmin -safemode leave

 # OUT=./hadoop dfs -test -f user/assignment1/mapreduceout/part-r-00000
 # echo "${OUT}"
#Q1
echo "clean Q1 output ..."
# if[ [./hadoop fs -test -f "user/assignment1/mapreduceout/part-r-00000" -eq 0] ]then
  ./hdfs dfs -rm -r -f /user/assignment1/userdataout/part-r-00000
# if[ (./hadoop fs -test -f /user/assignment1/mapreduceout/_SUCCESS) -eq 0 ]then
  ./hdfs dfs -rm -r -f /user/assignment1/userdataout/_SUCCESS
# if[ (./hadoop fs -test -d /user/assignment1/mapreduceout) -eq 0 ]then
  ./hdfs dfs -rmdir /user/assignment1/userdataout
echo "Q1 output removed "


#Q2
echo "clean Q2 output ..."
# if[ (./hadoop fs -test -f /user/assignment1/Q2out/part-r-00000) -eq 0 ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q2out/part-r-00000

# if[ (./hadoop fs -test -f /user/assignment1/Q2out/_SUCCESS) -eq 0 ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q2out/_SUCCESS

# if[ (./hadoop fs -test -f /user/assignment1/temp/part-r-00000) -eq 0 ]then
  ./hdfs dfs -rm -r -f /user/assignment1/temp/part-r-00000

# if[ (./hadoop fs -test -f /user/assignment1/temp/_SUCCESS) -eq 0 ]then
  ./hdfs dfs -rm -r -f /user/assignment1/temp/_SUCCESS

# if[ (./hadoop fs -test -d /user/assignment1/Q2out) -eq 0 ]then
  ./hdfs dfs -rmdir /user/assignment1/Q2out

# if[ (./hadoop fs -test -d /user/assignment1/temp) -eq ${0} ]then
  ./hdfs dfs -rmdir /user/assignment1/temp
echo "Q2 output removed "

#Q3
echo "clean Q3 output ..."
# if[ (./hadoop fs -test -f /user/assignment1/Q3out/part-r-00000) -eq ${0} ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q3out/part-r-00000

# if[ (./hadoop fs -test -f /user/assignment1/Q3out/_SUCCESS) -eq ${0} ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q3out/_SUCCESS

# if[ (./hadoop fs -test -f /user/assignment1/Q3temp/part-r-00000) -eq ${0} ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q3temp/part-r-00000

# if[ (./hadoop fs -test -f /user/assignment1/Q3temp/_SUCCESS) -eq ${0} ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q3temp/_SUCCESS

# if[ (./hadoop fs -test -d /user/assignment1/Q3out) -eq ${0} ]then
  ./hdfs dfs -rmdir /user/assignment1/Q3out

# if[ (./hadoop fs -test -d /user/assignment1/Q3temp) -eq ${0} ]then
  ./hdfs dfs -rmdir /user/assignment1/Q3temp
echo "Q3 output removed "


#Q4
echo "clean Q4 output ..."
# if[ (./hadoop fs -test -f /user/assignment1/Q4out/part-r-00000) -eq ${0} ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q4out/part-r-00000

# if[ (./hadoop fs -test -f /user/assignment1/Q4out/_SUCCESS) -eq ${0} ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q4out/_SUCCESS

# if[ (./hadoop fs -test -f /user/assignment1/Q4temp/part-r-00000) -eq ${0} ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q4temp/part-r-00000

# if[ (./hadoop fs -test -f /user/assignment1/Q4temp/_SUCCESS) -eq ${0} ]then
  ./hdfs dfs -rm -r -f /user/assignment1/Q4temp/_SUCCESS

# if[ (./hadoop fs -test -d /user/assignment1/Q4out) -eq ${0} ]then
  ./hdfs dfs -rmdir /user/assignment1/Q4out

# if[ (./hadoop fs -test -d /user/assignment1/Q4temp) -eq ${0} ]then
  ./hdfs dfs -rmdir /user/assignment1/Q4temp
echo "Q4 output removed "
