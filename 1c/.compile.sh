

path_in="$1"

C_PATH="/usr/lib/jvm/java-1.8.0/lib/tools.jar"
comp_version="$(date '+%s')"
path_out_a="output/a$comp_version"
path_out_b="output/b$comp_version"
save_file="scam.txt"

echo $path_in $path_out_a $path_out_b

cd ./
hadoop com.sun.tools.javac.Main -d ./ *.java
jar cf wc.jar Main*.class *.class 

hadoop jar wc.jar Main 10 2451392 2451894 $path_in $path_out_b &> $save_file
mv ./*.class ../bin
hdfs dfs -cat $path_out_b/part-r-00000 &>> $save_file


echo "Latest path $path_out_b"
#/user/$USER/input/rectangles10m
#hdfs dfs -put /dcs/cs346/tpcds/1G/store.dat input/1G/store
#hdfs dfs -put h input/1G/store_sales
#hdfs dfs -put /dcs/cs346/tpcds/40G/store.dat input/40G/store
#hdfs dfs -put /dcs/cs346/tpcds/40G/store_sales.dat input/40G/store_sales


