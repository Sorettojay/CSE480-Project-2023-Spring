#!/usr/bin/env python3
import argparse
import sys
import sqlite3
import os
import traceback

DB_FILE = "test.db"


class DatabaseException(Exception):
    pass


def run(module, sql_file, out_file, kwargs):
    conns = {}
    output = []
    for line in sql_file:
        line = line.strip()
        conn_name, command = line.split(':')
        if conn_name not in conns:
            conns[conn_name] = module.connect(DB_FILE, **kwargs)
        conn = conns[conn_name]
        try:
            output.append(line)
            output.extend(execute(command.strip(), conn))
        except:
            output.append("Database raised exception")
            break

    out_file.write("\n".join(output) + "\n")


def execute(statement, connection):
    def get_lines_from_result(result):
        if not result:
            return []

        output = [row for row in result]
        if not output:
            return []

        lines = ["/*"]
        for row in output:
            lines.append(str(row))
        lines.append("*/")
        return lines

    try:
        result = connection.execute(statement)
    except:
        print(traceback.print_exc(), file=sys.stderr)
        raise DatabaseException("Database raised exception")
    else:
        return get_lines_from_result(result)


def main():
    parser = argparse.ArgumentParser(description="""
    Project 4 command line interface.
    The script imports project.py and passes sql
    statements from a given file to it.
    If you pass the --sqlite argument,
    it will instead show you sqlite's output. 
    """)
    parser.add_argument('test_sql_file', type=argparse.FileType('r'),
                        help="the file containing sql statements")
    parser.add_argument('output_file', nargs='?', type=argparse.FileType('w'),
                        default=sys.stdout,
                        help="""the file to write the sqlite statements
                        and the database's output.
                        If not given, outputs to stdout""")
    parser.add_argument('--sqlite', action='store_true', help="""
    If given, runs the sqlite database instead of student's""")
    args = parser.parse_args()
    if args.sqlite:
        module = sqlite3
        kwargs = {"timeout": 0.1, "isolation_level": None}
    else:
        sys.modules['sqlite3'] = None
        import project
        module = project
        kwargs = {}
    run(module, args.test_sql_file, args.output_file, kwargs)
    try:
        os.remove(DB_FILE)
    except:
        pass

if __name__ == "__main__":
    main()
