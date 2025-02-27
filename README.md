# GoDBMS
GoDBMS is a simple database management system completely written in Go. It is built on top of a B-tree based key-value store. This project is developed using the "Build Your Own Database From Scratch in Go" guide, which can be found at [Build Your Own Database](https://build-your-own.org/database/).

## Features
- B-tree based key-value store
- Support for creating tables with multiple columns
- Insert, update, delete, and upsert operations
- Secondary indexes for efficient querying
- SQL-like query language for interacting with the database

## Installation
To install GoDBMS, clone the repository and build the project:

```sh
git clone https://github.com/yourusername/godbms.git
cd godbms
go build ./cmd/main.go
```

## Usage
To start using GoDBMS, run the built executable:

```sh
./main
```

### Example Queries
Here are examples of the query syntax supported by GoDBMS:

#### Table Creation
You can create tables with primary keys and indexes:

```sql
-- Multi-line format
create table demo (
    id int, 
    name bytes, 
    index (id),
    index (name), 
    primary key (id)
);

-- Single-line format
create table demo (id int, name bytes, index (id), index (name), primary key (id));
```

#### Insert Records
Insert new records into a table:

```sql
insert into demo (id, name) values (1, abcd);
```

#### Update Records
Update existing records using an index condition:

```sql
update demo set @name = hello index by name == abcd;
```

#### Delete Records
Delete records that match specified conditions:

```sql
delete from demo index by name == abcd;
```

#### Select Records
Retrieve data from tables:

```sql
select * from demo;
```


## Acknowledgements
This project is inspired by the "Build Your Own Database From Scratch in Go" guide. Special thanks to the authors for their excellent tutorial.
```
