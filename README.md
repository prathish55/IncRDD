# IncRDD
#IncRDD is a new custom RDD, created to accept and process the updates of the real-time data. 
#We also provide a few APIs to the application programmer to make updates to RDD elements. This includes add, update, and delete.

Compile spark code :
./sbt/bin/sbt clean
./sbt/bin/sbt package

Execute :
spark-submit --spark-submit --master yarn-cluster --verbose --executor-memory 4G --executor-cores 7 --num-executors 6 --class Test.SimpleApp target/scala-2.10/simple-project_2.10-1.0.jar 1 0 10000

#Arguments: <Choice save_to_file number_of_updates>
#Choice : 1-insert, 2- delete , 3- update
#save_to_file - 0- No, 1-Yes
#number_of_updates - any number




