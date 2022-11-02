import argparse
import itertools
import random
import sqlalchemy
import sqlalchemy.exc
from sqlalchemy.sql.functions import count
import sys
import time


backoff_idx = -1
backoff_amount = 0

def exponential_backoff(idx, rand=random):
    global backoff_idx
    global backoff_amount
    if idx != backoff_idx:
        # Reset delay if we have made progress
        backoff_amount = 0.1
    else:
        # Increase delay if we haven't
        backoff_amount *= 2

    # Random jitter
    delay = rand.uniform(backoff_amount / 2, backoff_amount)

    return delay


error_handlers = []


try:
    import psycopg2
except ImportError:
    pass
else:
    import psycopg2.errorcodes

    @error_handlers.append
    def retry_on_serialization_failure(idx, err, attempt):
        if (
            attempt < 3
            and isinstance(err, psycopg2.OperationalError)
            and err.orig.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE
       ):
            time.sleep(exponential_backoff(idx))
            return True


def main():
    parser = argparse.ArgumentParser('sqlalchemy-copy')
    parser.add_argument('--batch-size', action='store', type=int, default=100)
    parser.add_argument('source_url')
    parser.add_argument('target_url', nargs=argparse.OPTIONAL)
    parser.add_argument('tables', nargs=argparse.ZERO_OR_MORE)
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("Invalid batch size")

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
                    for row in itertools.islice(rows, args.batch_size)
                ]
                if not batch:
                    break
                attempt = 0
                while True:
                    try:
                        with tgt.begin():
                            tgt.execute(insert_stmt.values(batch))
                    except sqlalchemy.exc.DatabaseError as e:
                        handled = False
                        for handler in error_handlers:
                            if handler(idx, e, attempt):
                                attempt += 1
                                handled = True
                                break
                        if handled:
                            continue
                        else:
                            raise
                    else:
                        break
                idx += len(batch)
                print('%d / %d' % (idx, total_rows))


if __name__ == '__main__':
    main()
