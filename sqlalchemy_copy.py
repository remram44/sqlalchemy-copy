import argparse
import itertools
import sqlalchemy
from sqlalchemy.sql.functions import count
import sys


BATCH_SIZE = 100


def main():
    parser = argparse.ArgumentParser('sqlalchemy-copy')
    parser.add_argument('source_url')
    parser.add_argument('target_url', nargs=argparse.OPTIONAL)
    parser.add_argument('tables', nargs=argparse.ZERO_OR_MORE)
    args = parser.parse_args()

    # Connect to source
    src_engine = sqlalchemy.create_engine(args.source_url)

    # List source tables
    print("Tables in source:\n")
    src_metadata = sqlalchemy.MetaData(bind=src_engine)
    src_metadata.reflect()
    for table in src_metadata.sorted_tables:
        print('Table(%r)' % table.name)
        for col in table.columns:
            print('    %r' % col)
        for idx in table.indexes:
            print('    %r' % idx)

    # Connect to target
    if not args.target_url:
        return
    tgt_engine = sqlalchemy.create_engine(args.target_url)

    # Figure out tables to copy
    if args.tables:
        tables = args.tables
    else:
        tables = [t.name for t in src_metadata.sorted_tables]
    tgt_metadata = sqlalchemy.MetaData(bind=tgt_engine)
    tgt_metadata.reflect()
    missing = False
    for table in tables:
        if table not in src_metadata.tables:
            print(
                "Table %r doesn't exist in source" % table,
                file=sys.stderr,
            )
            missing = True
        if table not in tgt_metadata.tables:
            print(
                "Table %r doesn't exist in target" % table,
                file=sys.stderr,
            )
            missing = True
    if missing:
        sys.exit(1)

    # Copy
    with src_engine.connect() as src, tgt_engine.connect() as tgt:
        for table in tables:
            print("\nCopying %r" % table)
            src_table = src_metadata.tables[table]
            tgt_table = tgt_metadata.tables[table]
            total_rows = src.execute(
                sqlalchemy.select(count()).select_from(src_table)
            ).one()[0]
            rows = src.execute(src_table.select())
            insert_stmt = sqlalchemy.insert(tgt_table)
            idx = 0
            print('%d / %d' % (idx, total_rows))
            while True:
                batch = [
                    dict(row)
                    for row in itertools.islice(rows, BATCH_SIZE)
                ]
                if not batch:
                    break
                tgt.execute(insert_stmt.values(batch))
                idx += len(batch)
                print('%d / %d' % (idx, total_rows))


if __name__ == '__main__':
    main()
