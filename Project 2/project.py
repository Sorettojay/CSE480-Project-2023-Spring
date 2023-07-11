"""
Name: Jimmy Gray-Jones
Netid: grayjon4
PID: 159123092

How long did this project take you?:
Probably around 40 Hours. Willing to wager longer

Sources:
https://stackoverflow.com/questions/68311227/appending-to-a-python-global-list-of-dictionary
https://stackoverflow.com/questions/5212870/sorting-a-python-list-by-two-fields
https://scipython.com/book2/chapter-4-the-core-python-language-ii/questions/sorting-a-list-containing-none/
https://stackoverflow.com/questions/3766633/how-to-sort-with-lambda-in-python
https://www.w3schools.com/python/python_lambda.asp
https://docs.python.org/3/library/operator.html
https://www.digitalocean.com/community/tutorials/how-to-use-break-continue-and-pass-statements-when-working-with-loops-in-python-3
https://www.w3schools.com/python/python_dictionaries.asp

"""
import string
from operator import itemgetter

_ALL_DATABASES = {}

#################################

#Beginning of Row, Table, and Database Classes

def Row(data=[]):
    """
    Creates a row of data. Returns it as a list (Turned into a tuple later)
    Mostly used as an addition to the Table function
    """
    return data

class Table(object):
    """
    Creates a table

    Handles CREATE TABLE statements
    """

    # store tables in databases
    def __init__(self, name):
        self.name = str(name)
        self.cols = []
        self.rows = []
        self.datatypes = []

        # CAN DISPLAY CERTAIN COLUMNS BY CHECKING ITS DATATYPE
        # COLUMNS SHOULD DISPLAY ONLY THE DATATYPE THEY ARE APART OF
        # Figure out how to add column names and rows

    def ADD_TO_TABLE(self, data=[]):
        """
        Takes in a row object and appends it to our table

        Handles INSERT INTO statements
        """
        final_data = []

        x = 0

        if len(data) != len(self.cols):
            raise AssertionError('Length of Data != Length of Columns')

        for i in self.datatypes:
            if i == 'INTEGER':
                if data[x] == 'None':
                    data[x] = None
                    final_data.append(data[x])
                    x += 1
                else:
                    data[x] = int(data[x])
                    final_data.append(data[x])
                    x += 1

            if i == 'REAL':
                if data[x] == 'None':
                    data[x] = None
                    final_data.append(data[x])
                    x += 1
                else:
                    data[x] = float(data[x])
                    final_data.append(data[x])
                    x += 1

            if i == 'TEXT':
                if data[x] == 'None':
                    data[x] = None
                    final_data.append(data[x])
                    x += 1
                else:
                    data[x] = str(data[x])
                    final_data.append(data[x])
                    x += 1

            if i == 'NULL':
                data[x] = None
                final_data.append(data[x])
                x += 1

        self.rows.append(tuple(Row(final_data)))

    def DISPLAY_TABLE(self):
        """
        Displays all rows in the table
        """
        return self.rows

    def DISPLAY_COL_NAMES(self):
        """
        Displays the column headers of the table
        """
        return self.cols

    def SELECT_COL(self, names=(), order_by=()):
        """
        Displays specific columns

        Handles the "SELECT" statements
        """

        def ORDER_BY(names=(), cols=()):
            """
            If order_by flag is true, then orders designated
            columns from Ascending to Descending

            Used only in SELECT_COL()
            """
            total_requested_cols = []
            order_indexes = []
            col_indexes = []
            sorted_cols = []

            # Getting indexes of all the columns we want to show
            for i in names:
                col_indexes.append(self.cols.index(i))

            for i in cols:
                order_indexes.append(self.cols.index(i))

            if len(cols) == 1:
                sorted_cols = sorted(self.rows, key=lambda x: (x[order_indexes[0]]))
            if len(cols) == 2:
                sorted_cols = sorted(self.rows, key=lambda x: (x[order_indexes[0]], x[order_indexes[1]]))
            if len(cols) == 3:
                sorted_cols = sorted(self.rows,
                                     key=lambda x: (x[order_indexes[0]], x[order_indexes[1]], x[order_indexes[2]]))

            for i in range(len(self.rows)):
                requested_cols = []
                for k in col_indexes:
                    requested_cols.append(sorted_cols[i][k])
                total_requested_cols.append(tuple(requested_cols))
            return total_requested_cols

        if len(order_by) != 0:
            return ORDER_BY(names, order_by)

        total_requested_cols = []
        col_indexes = []

        for i in names:
            col_indexes.append(self.cols.index(i))

        for i in range(len(self.rows)):
            requested_cols = []
            for k in col_indexes:
                requested_cols.append(self.rows[i][k])
            total_requested_cols.append(tuple(requested_cols))
        return total_requested_cols

class Database(object):

    def __init__(self, name):
        self.name = name
        self.tables = {}

    def ADD_TABLE(self, name, Table):
        self.tables[name] = Table

    def SHOW_TABLES(self):
        return self.tables

#################################

def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)

###########################################################################

#TOKENIZATION CODE BELOW

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
    word = collect_characters(query, string.ascii_letters + "_" + string.digits)
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

def tokenize(query):
    tokens = []
    while query:
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue
            ##########################
        #Accounts for integers / stuff in parenthesis. #IS DIGIT ONLY RETURNS TRUE OR FALSE
        #Still need to account for floats

        #if query[0] in string.digits:
        if query[0].isdigit() is True:
            query = remove_word(query, tokens)
            continue

        if query[0] in ".":
            tokens.append(query[0])
            query = query[1:]
            continue

    ########################
        if query[0] in "(),*;":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if len(query) == len(old_query):
            break

    return tokens

#END OF TOKENIZATION CODE
##########################################################################

def float_handling(statement):
    """
    Handles floats in tokenized statements
    """
    holder = []
    to_remove = []

    # Code for cleaning up floats in code
    for i in range(len(statement)):
        statement[i] = str(statement[i])
        x = 0
        # HANDLING FLOAT STATEMENTS
        if statement[i].isdigit() is True:
            x = i
            if statement[i + 1] == '.':
                while statement[x]:
                    if statement[x] == ')' or statement[x] == ',':
                        holder.append(''.join(statement[i:x]))
                        to_remove.append(statement[i:x])
                        break
                    else:
                        x += 1
        holder.append(statement[i])

    for i in range(len(to_remove)):
        for k in range(len(to_remove[i])):
            holder.remove(to_remove[i][k])

    return holder

class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        pass

    def execute(self, statement):
        """
        Takes a SQL statement.

        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        #Tokenizing the query and cleaning the float values
        tokenized_statement = tokenize(statement)
        tokenized_statement = float_handling(tokenized_statement)

        #Handling create table statements
        if tokenized_statement[0] == 'CREATE' and tokenized_statement[1] == 'TABLE':
            start_index = tokenized_statement.index('(')
            end_index = tokenized_statement.index(')')
            table_name = tokenized_statement[2]
            datatypes = []
            cols = []

            for i in range(start_index + 1, end_index):

                # Adding Datatypes
                if tokenized_statement[i].isupper() is True and tokenized_statement[i] != ",":
                    datatypes.append(tokenized_statement[i])

                # Adding Column Names
                elif tokenized_statement[i].isupper() is False and tokenized_statement[i] != ",":
                    cols.append(tokenized_statement[i])

                # Skipping unwanted values
                else:
                    pass

            table = Table(table_name)
            table.cols = cols
            table.datatypes = datatypes
            database_name = table.name

            if database_name not in _ALL_DATABASES:
                #For now, database will be named the same thing as the table because
                #I cannot name it anything else for fear of it being overwritten
                database = Database(table.name)
                database.ADD_TABLE(table.name, table)
                _ALL_DATABASES[database.name] = database

        #Handling Insert Into statements
        if tokenized_statement[0] == 'INSERT' and tokenized_statement[1] == 'INTO':

            #Initializing all variables to be used
            values_to_be_added = []
            table_name = tokenized_statement[2]
            start_index = tokenized_statement.index('(')
            end_index = tokenized_statement.index(')')

            #Iterating from our start index, which is wherever the first '(' is detected + 1 value
            #Loop ends when ')' index is detected
            for i in range(start_index + 1, end_index):
                if tokenized_statement[i] != ')' and tokenized_statement[i] != ",":
                    values_to_be_added.append(tokenized_statement[i])

            #Using _ALL_DATABASES to access our desired table and add a Row of values to the table
            _ALL_DATABASES[table_name].tables[table_name].ADD_TO_TABLE(Row(values_to_be_added))

        #Handling Select statements + Clauses
        if tokenized_statement[0] == 'SELECT':
            #Initializing everything we need for further clauses
            start_index = tokenized_statement.index(tokenized_statement[1])
            selected_columns = []
            selected_tables = []
            show_all = False
            order_by = []
            table_name = None
            database_name = None

            #Iterates through the list, starting from the token after SELECT
            for i in range(start_index, len(tokenized_statement)):

                #Capturing only the columns we want
                if tokenized_statement[i].isupper() is False and tokenized_statement[i] != 'FROM' and tokenized_statement[i] != "," and tokenized_statement[i] != table_name:
                    selected_columns.append(tokenized_statement[i])

                #If asterisk is detetected, show_all flag becomes true
                if tokenized_statement[i] == "*":
                    show_all = True

                if tokenized_statement[i] == 'FROM':

                    #Appends selected tables to the "selected_tables" list
                    selected_tables.append(tokenized_statement[i + 1])

                    #Table name is also set to this as well
                    table_name = tokenized_statement[i + 1]

                    #For now, database name is set to table name
                    database_name = tokenized_statement[i + 1]

                #Handling ORDER BY statements
                if tokenized_statement[i] == 'ORDER':
                    if tokenized_statement[i + 1] == 'BY':

                        #Saving index for the while statement
                        index = i + 2
                        end_index = tokenized_statement.index(';')

                        for k in range(index, end_index):

                            if tokenized_statement[k] == ",":
                                continue
                            if tokenized_statement[k] == ';':
                                break
                            order_by.append(tokenized_statement[k])

                        #If show_all flag is true, show all columns in the table
                        #Else, show all selected columns in the table
                        if show_all == True:
                            k = _ALL_DATABASES[database_name].tables[table_name]
                            return _ALL_DATABASES[database_name].tables[table_name].SELECT_COL(k.cols, order_by)
                        else:
                            return _ALL_DATABASES[database_name].tables[table_name].SELECT_COL(selected_columns, order_by)

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass

#CREATE == PASSED
#INSERT == PASSED
#NULL TEST == PASSED
#ORDER TEST == PASSED
#ROWS == PASSED
#SELECT == PASSED
#TABLES == PASSED



#Why does my sorting work on the hardcoded test case, but not the sql outputs?

#Change the way I handle * command

