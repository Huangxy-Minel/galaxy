# function: compile .java and create jar

# add path of java file and output path 
TARGETPATH=/home/galaxy/galaxy/testfile/src
DESPATH=/home/galaxy/galaxy

# add lib path
GALAXYPATH=/home/galaxy/galaxy/
APACHECOMMON=/home/galaxy/hadoop-3.2.2/share/hadoop/common/hadoop-common-3.2.2.jar
APACHEMAPREDUCE=/home/galaxy/hadoop-3.2.2/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.2.2.jar
HIPPATH=/home/galaxy/code/java_code/lib/hip/hip-2.0.0.jar
LANGPATH=/home/galaxy/hadoop-3.2.2/share/hadoop/common/lib/commons-lang3-3.7.jar
COMMONCLI=/home/galaxy/hadoop-3.2.2/share/hadoop/common/lib/commons-cli-1.2.jar
FSPATH=/home/galaxy/hadoop-3.2.2/share/hadoop/hdfs/hadoop-hdfs-3.2.2.jar

# compile java file
javac -encoding utf-8 -cp $APACHECOMMON\
:$APACHEMAPREDUCE\
:$COMMONCLI\
:$FSPATH\
:$GALAXYPATH\
:$LANGPATH $TARGETPATH/Hello.java -d $DESPATH

# create jar
# jar cf wc2.jar WordCount*.class