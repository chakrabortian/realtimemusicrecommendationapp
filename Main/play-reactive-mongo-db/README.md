# Example application using play 2.5 and reactive mongo

# Configure MongoDB

Just change it in application.conf
```
mongodb.uri = "mongodb://localhost/persons"
```

# Run it
```
sbt run
```
# Cities

Cities controller uses a model, showing how simple it is in Play to parse json data from the clients

## Add some cities

```
curl -H "Content-Type: application/json" -X POST -d '{"name":"london","population": 8539000}' http://localhost:9000/cities/
curl -X "POST" http://localhost:9000/cities/add\?name\=Paris&population\=2244000
```
## Add some cities in bulk

```
curl -H "Content-Type: application/json" -X POST -d '[{"name":"tokyo","population": 13350000}, {"name":"osaka","population": 2665000}]' http://localhost:9000/cities/bulk
```

## Search cities

```
curl http://localhost:9000/cities?name=Paris
```

## See some errors

curl -H "Content-Type: application/json" -X POST -d '{"name":"london"}' http://localhost:9000/cities/

curl -H "Content-Type: application/json" -X POST -d '{"name":"london", "population": "x"}' http://localhost:9000/cities/

curl -H "Content-Type: application/json" -X POST -d '[{"name":"unknown"}]' http://localhost:9000/cities/bulk


# Persons

Person controller does not use a model, following the Coast to coast Json approach

## Add some persons

```
curl -H "Content-Type: application/json" -X POST -d '{"name":"jason","age": 12}' http://localhost:9000/persons/
curl -X "POST" http://localhost:9000/persons/add\?name\=Jason%20Voorhees\&age\=31
```

## Add some persons in bulk

```
curl -H "Content-Type: application/json" -X POST -d '[{"name":"jacob","age": 35}, {"name":"rohit","age": 27}, {"name":"federico","age": 31}, {"name":"oliver","age": 33}]' http://localhost:9000/persons/bulk
```

Test invalid formats
```
curl -H "Content-Type: application/json" -X POST -d '[{"name": "pepe"}]' http://localhost:9000/persons/bulk
```


## Search persons

```
curl http://localhost:9000/persons?name=jason
```

