import argparse
import sqlalchemy


def main():
    parser = argparse.ArgumentParser('sqlalchemy-copy')
    parser.add_argument('source_url')
    #parser.add_argument('target_url')
    args = parser.parse_args()

    src_engine = sqlalchemy.create_engine(args.source_url)


if __name__ == '__main__':
    main()
