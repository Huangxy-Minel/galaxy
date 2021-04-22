TARGETPATH=/home/galaxy/code/java_code/galaxy/util/src/
DESPATH=/home/galaxy/code/java_code/

GALAXYPATH=/home/galaxy/code/java_code/
LIBPATHGOOGLE=/home/galaxy/code/java_code/lib/google
LIBPATHJOBSIMPLE=/home/galaxy/code/java_code/lib/job_simple
LIBPATHCHECK=/home/galaxy/code/java_code/lib/check_framework
LIBPATHAPACHE=/home/galaxy/hadoop-3.2.2/share/hadoop/common/hadoop-common-3.2.2.jar
LANGPATH=/home/galaxy/hadoop-3.2.2/share/hadoop/common/lib/commons-lang3-3.7.jar

javac -encoding utf-8 -cp $LIBPATHGOOGLE/common/\
:$LIBPATHGOOGLE/error_prone/annotations/\
:$LIBPATHGOOGLE/j2objc/annotations/\
:$LIBPATHCHECK/checker/checker.jar\
:$LANGPATH\
:$GALAXYPATH\
:$LIBPATHJOBSIMPLE/\
:$LIBPATHAPACHE $TARGETPATH/HdfsIoUtils.java -d $DESPATH