# sqlalchemy-copy

This is a small script that copies data between any databases supported by SQLalchemy.

Example:

```
# List tables in source database
$ python sqlalchemy_copy.py sqlite:///test.sqlite3
Tables in source:

Table('people')
    Column('name', TEXT(), table=<people>, nullable=False)
    Column('age', INTEGER(), table=<people>)
    Index('idx_people_name', Column('name', TEXT(), table=<people>, nullable=False))

# Copy specific table
$ python sqlalchemy_copy.py sqlite:///test.sqlite3 postgresql://postgres:hackme@127.0.0.1/testdb --batch-size 50 people_table
Copying 'people_table'
0 / 72
50 / 72
72 / 72

# Copy all tables
$ python sqlalchemy_copy.py sqlite:///test.sqlite3 postgresql://postgres:hackme@127.0.0.1/testdb
Copying 'people_table'
0 / 72
72 / 72

Copying 'emails_table'
0 / 123
100 / 123
123 / 123
```
