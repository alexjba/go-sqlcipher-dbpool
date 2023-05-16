# go-sqlcipher-dbpool
Testing go-sqlcipher performance in different scenarios

## Test 1
#### MacOs, M1 Pro, 64GB Ram
```
Test info:  DB file 12,16 GB on disk;
Query: SELECT id, mentions FROM user_messages LIMIT ", max_rows
Nb of rows: 234329 
```

<img width="1395" alt="Screenshot 2023-05-16 at 17 32 30" src="https://github.com/alexjba/go-sqlcipher-dbpool/assets/47811206/b3ce6261-c583-4fe6-a0b8-77827fdfed8f">

<img width="489" alt="image" src="https://github.com/alexjba/go-sqlcipher-dbpool/assets/47811206/4132d482-0e75-4f11-92ef-87fc53335fb2">


## How to use:
Update `dbFile` and `dbPass` in main.go with DB info

Compile and run:

`go build main.go`

`./main -writes 0 -reads 2000 -writeOnDedicatedChannel 1 -dbDNSArgs "" -maxRows 50 -poolSize 1,2,3,4,5,10,30`
