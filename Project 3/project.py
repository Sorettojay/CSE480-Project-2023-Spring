"""
Name: Jimmy Gray-Jones
Time To Completion:
Comments:

Sources:
https://www.programiz.com/python-programming/methods/list/extend
https://www.w3schools.com/python/ref_func_all.asp
https://stackoverflow.com/questions/1740726/turn-string-into-operator

"""
import operator
import string
from operator import itemgetter

#FUNCTIONS I DONT KNOW:
#assert, zip, .format(), yield, extend, all

_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """

        #Feeds in tokens into the function, then creates a table
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []

            while True:
                column_name = tokens.pop(0)
                column_type = tokens.pop(0)
                #print(column_name, column_type)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            col_list = []
            col_indexes = []
            row_contents = []
            saved_col_indexes = []
            table = self.database.tables[table_name]
            if tokens[0] == ("("):
                tokens.pop(0)
                while True:
                    column = tokens.pop(0)
                    if tokens[0] == ',':
                        tokens.pop(0)
                    col_list.append(column)
                    col_indexes.append(table.column_names.index(column))
                    if tokens[0] == ")":
                        tokens.pop(0)
                        break
            pop_and_check(tokens, "VALUES")
            pop_and_check(tokens, "(")
            saved_col_indexes = col_indexes.copy()
            if col_list:
                row_contents = [None] * len(table.column_names)
            else:
                row_contents = row_contents
            while True:
                if col_list:
                    item = tokens.pop(0)
                    if col_indexes:
                        row_contents[col_indexes[0]] = item
                        col_indexes.pop(0)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        if len(tokens) != 0 and tokens[0] == ",":
                            col_indexes = saved_col_indexes
                            self.database.insert_into(table_name, row_contents)
                            row_contents = [None] * len(table.column_names)
                            tokens.pop(0)
                            tokens.pop(0)
                            continue
                        break
                    assert comma_or_close == ','
                else:
                    item = tokens.pop(0)
                    row_contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        if len(tokens) != 0 and tokens[0] == ",":
                            self.database.insert_into(table_name, row_contents)
                            row_contents = []
                            tokens.pop(0)
                            tokens.pop(0)
                            continue
                        break
                    assert comma_or_close == ','
            self.database.insert_into(table_name, row_contents)

        def where(tokens, table_name, delete_flag=False, update_flag=False):
            """
            Handles WHERE statements
            """
            where_col = None
            value = None
            oper = None
            table = self.database.tables[table_name]
            pop_and_check(tokens, "WHERE")
            operations = {'<': operator.lt, '>': operator.gt, '!': operator.ne, '=': operator.eq,
                          'ISNOT': operator.is_not, 'IS': operator.eq}
            # Handling Qualified table statements
            if tokens[0] == table_name:
                tokens.pop(0)
                tokens.pop(0)
                where_col = tokens.pop(0)
                if tokens[0] == '!':
                    oper = operations[tokens.pop(0)]
                    tokens.pop(0)
                    value = tokens.pop(0)
                elif tokens[0] == 'IS' and tokens[1] != 'NOT':
                    oper = operations[tokens.pop(0)]
                    value = tokens.pop(0)
                elif tokens[0] == 'IS' and tokens[1] == 'NOT':
                    oper = operations[tokens[0] + tokens[1]]
                    tokens.pop(0)
                    tokens.pop(0)
                    value = tokens.pop(0)
                else:
                    oper = operations[tokens.pop(0)]
                    value = tokens.pop(0)
            else:
                where_col = tokens.pop(0)
                if tokens[0] == '!':
                    oper = operations[tokens.pop(0)]
                    tokens.pop(0)
                    value = tokens.pop(0)
                elif tokens[0] == 'IS' and tokens[1] != 'NOT':
                    oper = operations[tokens.pop(0)]
                    value = tokens.pop(0)
                elif tokens[0] == 'IS' and tokens[1] == 'NOT':
                    oper = operations[tokens[0] + tokens[1]]
                    tokens.pop(0)
                    tokens.pop(0)
                    value = tokens.pop(0)
                else:
                    oper = operations[tokens.pop(0)]
                    value = tokens.pop(0)

            if where_col is not None and delete_flag is True:
                rows = table.rows
                where_list = []
                for i in rows:
                    # Specifically to handle comparing None Values, etc
                    if i[where_col] is None and value is not None:
                        continue
                    if oper(i[where_col], value) is False:
                        where_list.append(i)
                return where_list

            if where_col is not None:
                rows = table.rows
                where_list = []

                for i in rows:
                    #Specifically to handle comparing None Values, etc
                    if i[where_col] == None and value != None:
                        continue
                    if oper(i[where_col], value) is True:
                        where_list.append(tuple(i.values()))

                return where_list

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            pop_and_check(tokens, "SELECT")
            output_columns = []
            where_flag = False
            where_list = []
            while True:
                if tokens[1] == '.':
                    table_name = tokens.pop(0)
                    tokens.pop(0)
                    col = tokens.pop(0)
                    output_columns.append(col)
                    comma_or_from = tokens.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
                else:
                    col = tokens.pop(0)
                    output_columns.append(col)
                    comma_or_from = tokens.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
            table_name = tokens.pop(0)
            table = self.database.tables[table_name]
            if tokens[0] == 'WHERE':
                where_flag = True
                where_list = where(tokens, table_name, True)
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            while True:
                #Handling Qualified Table Names
                if tokens[0] == table_name:
                    tokens.pop(0)
                    tokens.pop(0)
                    col = tokens.pop(0)
                    order_by_columns.append(col)
                    if not tokens:
                        break
                    pop_and_check(tokens, ',')
                else:
                    col = tokens.pop(0)
                    order_by_columns.append(col)
                    if not tokens:
                        break
                    pop_and_check(tokens, ",")

            if where_flag is True:
                return where_list

            return self.database.select(output_columns, table_name, order_by_columns)

        def update(tokens):
            """
            Updates values in a table
            """
            operations = {'<': operator.lt, '>': operator.gt, '!': operator.ne, '=': operator.eq,
                          'ISNOT': operator.is_not, 'IS': operator.eq}

            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            table = self.database.tables[table_name]
            pop_and_check(tokens, "SET")
            col_name = tokens.pop(0)
            oper = operations[tokens.pop(0)]
            value = tokens.pop(0)

            if len(tokens) == 0:
                rows = table.rows
                updated_rows = []
                for i in rows:
                    i[col_name] = value
                    updated_rows.append(i)
                table.rows = updated_rows
            elif tokens[0] == 'WHERE':
                rows = table.rows
                where_rows = where(tokens, table_name)
                updated_rows = []
                for i in rows:
                    for k in where_rows:
                        if i[col_name] in k and i[col_name] not in updated_rows:
                            print(i[col_name], updated_rows, k)
                            #Because 4.0 is multiple values, its changing them all at once
                            i[col_name] = value
                            updated_rows.append(i)
                table.rows = updated_rows


        def delete(tokens):
            """
            Deletes values from the table
            """
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            table = self.database.tables[table_name]
            where_list = None
            where_col = None
            new_rows = None

            #No where value detected; Delete the entire table
            if len(tokens) == 0:
                self.database.delete_table(table_name)
            else:
                if tokens[0] == 'WHERE':
                    where_list = where(tokens, table_name, True)
                    new_rows = self.database.delete_row(table_name, where_list)

            return new_rows

        #tokenizing the initial statement
        tokens = tokenize(statement)

        #Assert that the first token is one of these 5
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "UPDATE", "DELETE"}

        last_semicolon = tokens.pop()
        assert last_semicolon == ";"
        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "SELECT":
            return select(tokens)
        elif tokens[0] == "UPDATE":
            return update(tokens)
        elif tokens[0] == "DELETE":
            return delete(tokens)
        else:
            assert not tokens

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    #DATA MANIPULATION COMMANDS
    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def insert_into(self, table_name, row_contents):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents)
        return []

    def select(self, output_columns, table_name, order_by_columns):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns)

    def delete_table(self, table_name):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.rows = []
        return table.rows

    def delete_row(self, table_name, row_contents):
        assert table_name in self.tables
        table = self.tables[table_name]
        return table.delete_row(row_contents)

    def update(self, table_name):
        assert table_name in self.tables
        table = self.tables[table_name]

    def left_outer_join(self):
        pass

class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        #zips column names and column types into a tuple
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents):
        #asserting that columns matches rows
        assert len(self.column_names) == len(row_contents)
        row = dict(zip(self.column_names, row_contents))
        self.rows.append(row)

    def select_rows(self, output_columns, order_by_columns):

        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):
            return sorted(self.rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        #Seeing if a * is detected and displaying data accordingly
        expanded_output_columns = expand_star_column(output_columns)

        #Seeing if the output columns exist
        check_columns_exist(expanded_output_columns)

        #Seeing if the columns to order_by, exist
        check_columns_exist(order_by_columns)

        #Sorting rows
        sorted_rows = sort_rows(order_by_columns)

        return generate_tuples(sorted_rows, expanded_output_columns)

    def delete_row(self, row_contents):
        self.rows = row_contents


def pop_and_check(tokens, same_as):
    #Current item == first item popped off list of tokens
    item = tokens.pop(0)

    #Makes sure that the list is not empty?
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        #print("Query:{}".format(query))
        #print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),*;.!=><":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens

#CURRENT ISSUE:
#Qualified Statements

#test.delete.01.sql == PASSED
#test.delete.02.sql == PASSED
#test.distinct.01.sql ==
#test.distinct.02.sql ==
#test.regression.01.sql == PASSED
#test.regression.02.sql == PASSED
#test.ids.01.sql == PASSED
#test.ids.02.sql == PASSED
#test.ids.03.sql == PASSED
#test.ids.04.sql == PASSED
#test.ids.05.sql ==
#test.insert-columns.01.sql == PASSED
#test.insert-columns.02.sql == PASSED
#test.insert-columns.03.sql == PASSED
#test.join.01.sql ==
#test.join.02.sql ==
#test.join.03.sql ==
#test.multi-insert.01.sql == PASS
#test.multi-insert.02.sql == PASS
#test.multi-insert.03.sql == PASS
#test.qualified.01.sql == PASS
#test.qualified.02.sql == PASS
#test.qualified.03.sql == PASS
#test.regression.01.sql == PASS
#test.regression.02.sql == PASS
#test.update.01.sql ==
#test.update.02.sql ==
#test.update.03.sql ==
#test.where.01.sql == PASS
#test.where.02.sql == PASS
#test.where.03.sql == PASS
#test.where.04.sql == PASS
#test.where.05.sql == PASS

