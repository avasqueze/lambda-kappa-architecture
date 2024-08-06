#!/bin/bash

# Define the path to the mapred-site.xml file
MAPRED_SITE_XML="/opt/hadoop/etc/hadoop/mapred-site.xml"

# Write the configuration to the mapred-site.xml file
cat << EOF > "$MAPRED_SITE_XML"
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=\${HADOOP_HOME}</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=\${HADOOP_HOME}</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=\${HADOOP_HOME}</value>
    </property>
</configuration>
EOF

echo "mapred-site.xml file has been rewritten."