# Derived from official mysql image (our base image)
FROM mysql

# Add a database
ENV MYSQL_DATABASE morph_example

# Add the content of the sql-scripts/ directory to your image
# All scripts in docker-entrypoint-initdb.d/ are automatically
# executed during container startup
COPY ./ /docker-entrypoint-initdb.d/
#ADD https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-mysql/sql-scripts/morph_example.sql /sql-scripts/morph_example.sql
#RUN cp /sql-scripts/* /docker-entrypoint-initdb.d/
