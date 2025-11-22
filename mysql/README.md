# Setup
```
docker-compose up -d
```

# Data load
```
cd data;
docker exec -i mysql-local mysql -u root -proot < lahman2016.sql
```