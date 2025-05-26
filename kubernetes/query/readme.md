# Trino Query Engine
```SQL
SHOW catalogs;
```

### Create schema for delta tables
```sql
CREATE SCHEMA delta_stream.customers;
CREATE SCHEMA delta_stream.rides;
CREATE SCHEMA delta_stream.drivers;
CREATE SCHEMA delta_stream.yelp;
```

### use register procedure to register existing delta tables
```sql
CALL delta_stream.system.register_table(
schema_name => 'customers',
table_name => 'customers',
table_location => 's3a://stream/delta/customers'
);

CALL delta_stream.system.register_table(
schema_name => 'rides',
table_name => 'rides',
table_location => 's3a://stream/delta/rides'
);

CALL delta_stream.system.register_table(
schema_name => 'drivers',
table_name => 'drivers',
table_location => 's3a://stream/delta/drivers'
);

CALL delta_stream.system.register_table(
schema_name => 'yelp',
table_name => 'yelp_business',
table_location => 's3a://production/yelp/business'
);
```

### querying new Delta Tables 
```sql
SELECT * FROM delta_stream.customers.customers;
SELECT count(*) FROM delta_stream.customers.customers;

SELECT * FROM delta_stream.rides.rides;
SELECT count(*) FROM delta_stream.rides.rides;

SELECT * FROM delta_stream.drivers.drivers;
SELECT count(*) FROM delta_stream.rides.drivers;

SELECT * FROM delta_stream.yelp.yelp_business;
SELECT count(*) FROM delta_stream.yelp.yelp_business;
```



