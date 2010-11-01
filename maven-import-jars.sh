mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=hadoop-gpl-compression -Dversion=0.2.0 -Dpackaging=jar -Dfile=./lib/hadoop-gpl-compression-0.2.0-dev.jar
g

mvn install:install-file -Dfile=./lib/hbase-0.20.6.jar -DgroupId=org.apache.hbase -DartifactId=hbase -Dversion=0.20.6 -DgeneratedPom=true -Dpackaging=jar

mvn install:install-file -Dfile=./lib/zookeeper-3.2.2.jar -DgroupId=org.apache -DartifactId=zookeeper -Dversion=3.2.2 -DgeneratedPom=true -Dpackaging=jar

