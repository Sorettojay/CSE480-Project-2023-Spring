"""
Name: Jimmy Gray-Jones
Time To Completion:
Comments:

Sources:
https://www.programiz.com/python-programming/methods/list/extend
https://www.w3schools.com/python/ref_func_all.asp
https://stackoverflow.com/questions/1740726/turn-string-into-operator
https://stackoverflow.com/questions/6475321/global-variable-python-classes

"""
import copy
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
        #Make a variable to keep track of whether you're on a copy or not
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    #Store these on database objects
    autocommit = True
    transaction = None  # Class Attribute Used for my transactions
    immediate_transaction = None
    exclusive_transaction = None

    #For now, all locks will be boolean values
    reserved_lock = set()
    exclusive_lock = None
    shared_lock = None #Shared locks block exclusive locks

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
            table_name = None
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            if tokens[0] == 'IF':
                pop_and_check(tokens, 'IF')
                pop_and_check(tokens, 'NOT')
                pop_and_check(tokens, 'EXISTS')
                table_name = tokens.pop(0)
                if table_name in self.database.tables:
                    return
            if table_name is None:
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
            if table_name not in self.database.tables:
                raise Exception('no such table'+' '+table_name)
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

        def drop_table(tokens):
            pop_and_check(tokens, "DROP")
            pop_and_check(tokens, "TABLE")
            if tokens[0] != "IF":
                table_name = tokens.pop(0)
                self.database.tables.pop(table_name)
            else:
                pop_and_check(tokens, "IF")
                pop_and_check(tokens, "EXISTS")
                table_name = tokens.pop(0)
                if table_name in self.database.tables:
                    self.database.tables.pop(table_name)

        #tokenizing the initial statement
        tokens = tokenize(statement)

        #Assert that the first token is one of these 5
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "UPDATE", "DELETE",
                             "BEGIN", "COMMIT", "ROLLBACK", "DROP"}

        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            if Connection.autocommit is True:
                insert(tokens)
                return []
            elif Connection.transaction is not None:
                #If database object is not the same as the last object in the set
                if list(Connection.reserved_lock)[-1] != Connection.transaction:
                    raise Exception('database is locked')
                if Connection.exclusive_transaction is not None:
                    raise Exception('database is locked')
                insert(tokens)
                return []
        elif tokens[0] == "SELECT":
            if Connection.autocommit is True:
                return select(tokens)
            elif Connection.transaction is not None:
                if Connection.exclusive_lock is not None:
                    if Connection.shared_lock is True:
                        raise Exception('database is locked')
                Connection.shared_lock = True
                return select(tokens)
            elif Connection.exclusive_lock is not None:
                if Connection.shared_lock is True:
                    raise Exception('database is locked')
                return select(tokens)
            elif Connection.immediate_transaction is not None:
                Connection.shared_lock = True
                return select(tokens)
        elif tokens[0] == "UPDATE":
            if Connection.autocommit is True:
                return update(tokens)
            elif Connection.transaction is not None:
                if list(Connection.reserved_lock)[-1] != Connection.transaction:
                    raise Exception('database is locked')
                if Connection.exclusive_transaction is not None:
                    raise Exception('database is locked')
                return update(tokens)
        elif tokens[0] == "DELETE":
            return delete(tokens)
        elif tokens[0] == "DROP" and tokens[1] == "TABLE":
            return drop_table(tokens)
        elif tokens[0] == "BEGIN":
            pop_and_check(tokens, "BEGIN")
            if tokens[0] == "TRANSACTION":

                #creating a deep copy of the database // Represents a different connection to table
                #Connection.transaction is a global variable within my class, and I change it like this
                Connection.transaction = copy.deepcopy(self.database)

                #Adding this new database copy to the list;
                #Each instance of a is a different deep copy of the original database in the MEMORY
                #So between transactions, supposedly, a can be remembered between transactions
                _ALL_DATABASES[self.transaction.filename] = Connection.transaction

                #Turning off Autocommit
                Connection.autocommit = False

                Connection.reserved_lock.add(Connection.transaction)

            elif tokens[0] == "DEFERRED" and tokens[1] == "TRANSACTION":
                #The same as just an empty transaction statement
                if Connection.autocommit is False:
                    raise Exception('Database is locked')
                Connection.autocommit = False
                Connection.transaction = copy.deepcopy(self.database)
                _ALL_DATABASES[self.transaction.filename] = Connection.transaction
                Connection.reserved_lock.add(Connection.transaction)
            elif tokens[0] == "EXCLUSIVE" and tokens[1] == "TRANSACTION":
                #Autocommit should be true after every commit, so this should be fine
                if Connection.autocommit is False:
                    raise Exception('Database is locked')
                Connection.exclusive_lock = True
                Connection.autocommit = False
                Connection.exclusive_transaction = copy.deepcopy(self.database)
                Connection.reserved_lock.add(Connection.exclusive_transaction)
                _ALL_DATABASES[self.exclusive_transaction.filename] = Connection.exclusive_transaction
            elif tokens[0] == "IMMEDIATE" and tokens[1] == "TRANSACTION":
                Connection.reserved_lock.add(Connection.immediate_transaction)
                Connection.autocommit = False
                Connection.immediate_transaction = copy.deepcopy(self.database)
                _ALL_DATABASES[self.immediate_transaction.filename] = Connection.immediate_transaction

        elif tokens[0] == "COMMIT":

            #No prior commits made
            if Connection.autocommit is True:
                raise Exception('COMMIT statement attempted without prior BEGIN statement')

            #IMMEDIATE TRANSACTION CAN HAPPEN BEFORE DEFERRED/NORMAL TRANSACTIONS
            if Connection.transaction is not None and Connection.immediate_transaction is not None:
                raise Exception('database is locked')

            #Committing transactions to original database
            _ALL_DATABASES[self.database.filename] = Connection.transaction

            #Returning all locks and shit back to original state
            if Connection.transaction is not None:
                if len(Connection.reserved_lock) == 1:
                    Connection.reserved_lock = set()
                else:
                    Connection.reserved_lock = Connection.reserved_lock.discard(self.transaction)
            if Connection.immediate_transaction is not None:
                if len(Connection.reserved_lock) == 1:
                    Connection.reserved_lock = set()
                else:
                    Connection.reserved_lock = Connection.reserved_lock.discard(self.immediate_transaction)
            if Connection.exclusive_transaction is not None:
                if len(Connection.reserved_lock) == 1:
                    Connection.reserved_lock = set()
                else:
                    Connection.reserved_lock = Connection.reserved_lock.discard(self.exclusive_transaction)

            Connection.transaction = None
            Connection.exclusive_lock = None
            Connection.shared_lock = None
            Connection.autocommit = True
            deferred_transaction = None
            immediate_transaction = None
            exclusive_transaction = None

        elif tokens[0] == "ROLLBACK":
            if Connection.autocommit is True:
                raise Exception('Can only rollback within a manual transaction')

            #Setting locks to None again, because it's rolling back the transaction completely

            if Connection.transaction is not None:
                self.__init__(self.database.filename)
                Connection.exclusive_lock = None
                if len(Connection.reserved_lock) == 1:
                    Connection.reserved_lock = set()
                else:
                    Connection.reserved_lock = Connection.reserved_lock.discard(self.transaction)
                Connection.shared_lock = None
                Connection.transaction = None
                Connection.autocommit = True
            if Connection.immediate_transaction is not None:
                self.__init__(self.database.filename)
                if len(Connection.reserved_lock) == 1:
                    Connection.reserved_lock = set()
                else:
                    Connection.reserved_lock = Connection.reserved_lock.discard(self.immediate_transaction)
                Connection.immediate_transaction = None
                Connection.autocommit = True
            if Connection.exclusive_lock is not None:
                self.__init__(self.database.filename)
                if len(Connection.reserved_lock) == 1:
                    Connection.reserved_lock = set()
                else:
                    Connection.reserved_lock = Connection.reserved_lock.discard(self.exclusive_transaction)
                Connection.exclusive_lock = None
                Connection.exclusive_transaction = None
                Connection.autocommit = True

        else:
            assert not tokens


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


#PROJECT 4 TEST CASES
#test.connections.01.sql == PASS
#test.connections.02.sql == PASS
#test.create_drop_table.01.sql == PASS
#test.create_drop_table.02.sql == PASS
#test.create_drop_table.03.sql == PASS
#test.isolation.01.sql == PASS
#test.isolation.02.sql ==
#test.regression.01.sql == PASS
#test.regression.02.sql ==
#test.rollback.01.sql == PASS
#test.rollback.02.sql == PASS
#test.transaction_modes.01.sql == PASS
#test.transaction_modes.02.sql == PASS
#test.transactions_modes.03.sql == PASS
#test.transactions.01.sql == PASS
#test.transactions.02.sql == PASS
#test.transactions.03.sql == PASS



