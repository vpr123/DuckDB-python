from duckdb import sql

def say(txt):
    print("==> " + txt)

def getSQL(file_path):
    file = open(file_path, "r")
    content = file.read()
    file.close()
    return content

def runSQL (file_path, params=None):
    s = getSQL(file_path)
    if params is not None:
        for k, v in params.items():
           s = s.replace("$$" + k + "$$", v);
    say(file_path+"\n")
    print(s)
    return sql(s)

def runQuery (query):
    say(query+"\n")
    return sql(query)