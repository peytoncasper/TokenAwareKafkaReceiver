CREATE KEYSPACE ups WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

CREATE TABLE ups.test(
    test1 text,
    test2 text,
    PRIMARY KEY(test1)
);