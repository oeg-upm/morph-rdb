FROM ubuntu:18.04
RUN apt-get update && apt-get install -y openjdk-8-jdk nano less git maven
RUN mkdir /morphrdb
COPY . /morphrdb
RUN cd /morphrdb && mvn clean install
RUN cp -r /morphrdb/morph-rdb-dist/target/*.jar /
RUN cp -r /morphrdb/morph-examples/dependency /
RUN cp -r /morphrdb/run-docker.sh /
CMD ["tail", "-f", "/dev/null"]
#CMD ["/bin/bash", "mysql-example1-batch.sh"]
#CMD ["/usr/bin/java", "-cp", ".:morph-rdb.jar:lib/*:dependency/*", "es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner", "examples-mysql", "example1-batch-mysql.morph.properties"]
#CMD ["/usr/bin/java", "-cp", ".:morph-rdb.jar:lib/*:dependency/*", "es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphCSVRunner", "examples-csv", "example1-batch-csv.morph.properties"]
