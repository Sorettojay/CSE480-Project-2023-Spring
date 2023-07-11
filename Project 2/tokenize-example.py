import string

query = " INSERT   INTO instructors VALUES('James', 29, 17.5, NULL);"

correct_tokens = [
    "INSERT",
    "INTO",
    "instructors",
    "VALUES",
    "(",
    "James",
    ",",
    29,
    ",",
    17.5,
    ",",
    None,
    ")",
    ";"
]

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


def tokenize(query):
    tokens = []
    while query:
        print("Query:{}".format(query))
        print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        #todo integers, floats, misc. query stuff (select * for example)

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens

tokens = tokenize(query)
print(tokens)
print(correct_tokens)