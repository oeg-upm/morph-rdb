# Derived from official mysql image (our base image)
FROM ubuntu

# Add the content of the sql-scripts/ directory to your image
# All scripts in docker-entrypoint-initdb.d/ are automatically
# executed during container startup
WORKDIR /app

ADD https://github.com/oeg-upm/morph-rdb/releases/download/morph-RDB_v3.9.17/morph-rdb-dist-3.9.17.jar /app
ADD https://github.com/oeg-upm/morph-rdb/releases/download/morph-RDB_v3.9.17/dependency.zip /app
ADD https://github.com/oeg-upm/morph-rdb/releases/download/morph-RDB_v3.9.17/examples-mysql.zip /app

RUN apt-get update &&  apt-get install -y openjdk-8-jdk unzip lsof vim net-tools iputils-ping wget mysql-client

RUN cd /app && unzip dependency.zip  && unzip examples-mysql.zip && mv morph-rdb-dist-3.9.17.jar  morph-rdb.jar

# Define default command.
CMD ["tail","-f","/dev/null"]
