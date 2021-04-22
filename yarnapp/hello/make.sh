# function: compile .java and create jar

# add path of java file and output path 
TARGETPATH=/home/galaxy/galaxy/yarnapp/hello/src
DESPATH=/home/galaxy

# add lib path
GALAXYPATH=/home/galaxy/galaxy
APACHECOMMON=/home/galaxy/hadoop-3.2.2/share/hadoop/common/hadoop-common-3.2.2.jar
APACHEMAPREDUCE=/home/galaxy/hadoop-3.2.2/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.2.2.jar
HIPPATH=/home/galaxy/code/java_code/lib/hip/hip-2.0.0.jar
LANGPATH=/home/galaxy/hadoop-3.2.2/share/hadoop/common/lib/commons-lang3-3.7.jar
COMMONCLI=/home/galaxy/hadoop-3.2.2/share/hadoop/common/lib/commons-cli-1.2.jar
GOOGLECOMMON=/home/galaxy/code/java_code/lib/google/common/
FSPATH=/home/galaxy/hadoop-3.2.2/share/hadoop/hdfs/hadoop-hdfs-3.2.2.jar
YARNAPI=/home/galaxy/hadoop-3.2.2/share/hadoop/yarn/hadoop-yarn-api-3.2.2.jar
YARNCLI=/home/galaxy/hadoop-3.2.2/share/hadoop/yarn/hadoop-yarn-client-3.2.2.jar
YARNCOMMON=/home/galaxy/hadoop-3.2.2/share/hadoop/yarn/hadoop-yarn-common-3.2.2.jar

# compile java file
javac -encoding utf-8 -cp $APACHECOMMON\
:$APACHEMAPREDUCE\
:$COMMONCLI\
:$GOOGLECOMMON\
:$FSPATH\
:$YARNAPI\
:$YARNCLI\
:$YARNCOMMON\
:$GALAXYPATH\
 $TARGETPATH/Client.java -d $DESPATH

# create jar
# jar cf wc2.jar WordCount2*.class