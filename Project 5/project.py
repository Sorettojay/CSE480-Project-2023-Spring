"""
Name: Jimmy Gray-Jones
NetID: grayjon4
Time To Completion: 20-30 hours?
Comments: Tired

Sources:
https://www.w3schools.com/python/python_lists_comprehension.asp

"""
import copy
import operator
import string
from operator import itemgetter

#FUNCTIONS I DONT KNOW:
#assert, zip, .format(), yield, extend, all

_ALL_DATABASES = {}
_ALL_TRANSACTIONS = {}


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

    default_value = None
    default_value_index = None
    view = None

    def execute(self, statement, params=None):
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
            if tokens[0] == 'IF':
                pop_and_check(tokens, 'IF')
                pop_and_check(tokens, 'NOT')
                pop_and_check(tokens, 'EXISTS')
                table_name = tokens.pop(0)
                if table_name in self.database.tables:
                    return
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []

            while True:
                column_name = tokens.pop(0)
                column_type = tokens.pop(0)
                if tokens[0] == 'DEFAULT':
                    pop_and_check(tokens, "DEFAULT")
                    self.default_value = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                if self.default_value is not None:
                    self.default_value_index = column_name_type_pairs.index((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','

            if table_name in self.database.tables:
                raise Exception(" ".join(("table", table_name, "already exists")))

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
            if tokens[0] == "DEFAULT":
                pop_and_check(tokens, "DEFAULT")
                pop_and_check(tokens, "VALUES")
                row_contents = [None] * len(table.column_names)
                row_contents[self.default_value_index] = self.default_value
                self.database.insert_into(table_name, row_contents)
                return
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
                        for k in range(len(row_contents)):
                            if self.default_value is not None and row_contents[k] is None:
                                if row_contents[self.default_value_index] is None:
                                    row_contents[k] = self.default_value
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
                    if i[where_col] is None and value is not None:
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
            max_flag = False
            min_flag = False
            desc_flag = False
            self.default_value = None
            self.default_value_index = None
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
                elif tokens[0] == 'MAX':
                    max_flag = True
                    tokens.pop(0)
                    tokens.pop(0)
                    col = tokens.pop(0)
                    output_columns.append(col)
                    tokens.pop(0)
                    comma_or_from = tokens.pop(0)
                    if comma_or_from == "FROM":
                        break
                    assert comma_or_from == ','
                elif tokens[0] == 'MIN':
                    min_flag = True
                    tokens.pop(0)
                    tokens.pop(0)
                    col = tokens.pop(0)
                    output_columns.append(col)
                    tokens.pop(0)
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
                where_list = where(tokens, table_name)
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
                    if tokens:
                        if tokens[0] == 'DESC':
                            desc_flag = True
                            break
                    if not tokens:
                        break
                    pop_and_check(tokens, ',')
                else:
                    col = tokens.pop(0)
                    order_by_columns.append(col)
                    if tokens:
                        if tokens[0] == 'DESC':
                            desc_flag = True
                            break
                    if not tokens:
                        break
                    pop_and_check(tokens, ",")

            if desc_flag is True:
                return self.database.select(output_columns, table_name, order_by_columns, True)

            if where_flag is True:
                if max_flag is True:
                    _max_(where_list)
                if col != '*':
                    w_l = list()
                    index = table.column_names.index(col)
                    for i in where_list:
                        f = tuple((i[index],))
                        w_l.append(f)
                    return w_l

                return where_list

            if max_flag is True:
                return _max_(self.database.select(output_columns, table_name, order_by_columns))

            if min_flag is True:
                return _min_(self.database.select(output_columns, table_name, order_by_columns))

            if table_name == self.view:
                [*data] = self.database.select(output_columns, table_name, order_by_columns)
                final = []
                for i in data:
                    final.append(tuple((i[0],)))
                return final

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

        def _max_(col):
            """
            Returns the max value in a given column
            """
            max_ = []
            k = max(col)
            max_.append(tuple(k))
            return max_

        def _min_(col):
            """
            Returns the max value in a given column
            """
            min_ = []
            k = min(col)
            min_.append(tuple(k))
            return min_

        def create_view(tokens):
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "VIEW")
            self.view = None
            self.view = tokens.pop(0)
            pop_and_check(tokens, "AS")

            view = None
            origin = self.database.tables[tokens[3]]

            if tokens[1] != '*':
                col = tokens[1]
                view = self.database.tables[self.view] = origin
                comparison = select(tokens)
                new_list = [i for i in view.rows if (i[col],) in comparison]
                view.rows = new_list
            else:
                self.database.tables[self.view] = origin

        #tokenizing the initial statement
        if params is not None:
            tokens = params
        else:
            tokens = tokenize(statement)

        #Assert that the first token is one of these 5
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "UPDATE", "DELETE"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            if tokens[1] == "TABLE":
                create_table(tokens)
                return []
            elif tokens[1] == "VIEW":
                create_view(tokens)
                return
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

    def executemany(self, wildcard_statement, wildcard_values):

        #Print statements occur at the beginning of all statements for some reason
        tokens = tokenize(wildcard_statement)
        params = []
        new_tokens = []

        n = len(wildcard_values[0])

        if n == 2:
            for i in range(len(wildcard_values)):
                vals = wildcard_values.pop(0)
                a, b = vals
                params.append('(')
                params.append(a)
                params.append(',')
                params.append(b)
                params.append(')')
                params.append(',')

        if n == 3:
            for i in range(len(wildcard_values)):
                vals = wildcard_values.pop(0)
                a, b, c = vals
                params.append('(')
                params.append(a)
                params.append(',')
                params.append(b)
                params.append(',')
                params.append(c)
                params.append(')')
                params.append(',')

        params.pop(-1) #Removing the last comma from the params so errors are not thrown

        for i in range(len(tokens)):
            if tokens[i-1] == "VALUES":
                for k in range(len(params)):
                    new_tokens.append(params[k])
            else:
                if tokens[i] == '?' or tokens[i] == '(' or tokens[i] == ')' or tokens[i] == ',':
                    i = i
                else:
                    new_tokens.append(tokens[i])

        return self.execute(wildcard_statement, new_tokens)

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename, timeout=None, isolation_level=None):
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

    def select(self, output_columns, table_name, order_by_columns, reverse=False):
        assert table_name in self.tables
        table = self.tables[table_name]
        if reverse is True:
            return table.select_rows(output_columns, order_by_columns, True)
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

    def select_rows(self, output_columns, order_by_columns, reverse=False):

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
            if reverse is True:
                return sorted(self.rows, key=itemgetter(*order_by_columns),reverse=True)
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

        if query[0] in "(),*;.!=><?":
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


#test.aggregate.01.sql == PASS
#test.aggregate.02.sql == PASS
#test.aggregate.03.sql == PASS
#test.default.01.sql == PASS
#test.default.02.sql == PASS
#test.desc.01.sql == PASS
#test.desc.02.sql == PASS
#test.parameters.01.sql == PASS
#test.parameters.02.sql == PASS
#test.regression.01.sql ==
#test.regression.02.sql == PASS
#test.view.01 == PASS
#test.view.02 == PASS
#test.view.03 == PASS