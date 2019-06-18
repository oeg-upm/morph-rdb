docker stop mysql-morph
docker rm mysql-morph
docker build -t mysql-morph .
docker run -d -p 3307:3306 --name mysql-morph -e MYSQL_ROOT_PASSWORD=password mysql-morph
docker start mysql-morph
docker ps -a
