#!/usr/bin/env python3
import argparse
import sys


def run(module, sql_file, out_file):
    conn = module.connect(":memory:")
    output = []
    #    with open(sql_file, 'r') as sql_handle:
    for line in sql_file:
        output += execute(line.strip(), conn)
        #    with open(out_file, 'w') as out_handle:
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

    lines = [statement]
    result = connection.execute(statement)
    lines.extend(get_lines_from_result(result))
    return lines


def main():
    parser = argparse.ArgumentParser(description="""
    The script imports project.py and passes sql
    statements from a given file to it.
    If you pass the --sqlite argument, it will instead
    show you sqlite's output.
    """)
    parser.add_argument('test_sql_file', type=argparse.FileType('r'),
                        help="the file containing sql statements")
    parser.add_argument('output_file', nargs='?', type=argparse.FileType('w'),
                        default=sys.stdout,
                        help="""the file to write the sqlite statements
                        and the database's output. If not given,
                        outputs to stdout""")
    parser.add_argument('--sqlite', action='store_true', help="""
    If given, runs the sqlite database instead of student's""")
    args = parser.parse_args()

    if args.sqlite:
        import sqlite3
        module = sqlite3
    else:
        sys.modules['sqlite3'] = None
        import project
        module = project
    run(module, args.test_sql_file, args.output_file)

if __name__ == "__main__":
    main()
