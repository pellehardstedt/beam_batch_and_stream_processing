C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server-1.properties
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server-2.properties


docker run -d --rm -p 9000:9000 \
    -e KAFKA_BROKERCONNECT=localhost:9092,localhost:9093,localhost:9094 \
    -e JVM_OPTS="-Xms32M -Xmx64M" \
    -e SERVER_SERVLET_CONTEXTPATH="/" \
    obsidiandynamics/kafdrop