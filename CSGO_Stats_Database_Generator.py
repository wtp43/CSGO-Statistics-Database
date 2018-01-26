import sqlite3
from sqlite3 import Error
import csv
import glob


def createConnection(db):
    try:
        conn = sqlite3.connect(db)
        return conn
    except Error as e:
        print(e)
    return None


def createTeamTable(teamName, conn):
    try:
        with open('{}*match-stats.csv'.format(teamName)) as file:
            dr = csv.DictReader(file)
            to_db = [(i['matchDate'], i['event'], i['map'],i['score'],i['opponent'],i['opponentScore'],i['result'],
                      i['type'], i['matchId']) for i in dr]
        teamName = ''.join(e for e in teamName if e.isalnum())
        createTableSql = """ CREATE TABLE IF NOT EXISTS {} (
                                            matchDate TEXT ,
                                            event TEXT,
                                            map TEXT,
                                            score INTEGER,
                                            opponent TEXT,
                                            opponentScore INTEGER,
                                            result TEXT,
                                            type TEXT,
                                            matchId FLOAT,
                                            UNIQUE (matchId)
                                        ); """.format(teamName)
        c = conn.cursor()
        c.execute(createTableSql)
        c.executemany(""" INSERT OR IGNORE INTO {} (
                                matchDate, event, map, score, 
                                opponent, opponentScore, result, type, matchId) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);""".format(teamName), to_db)
        conn.commit()
    except Error as e:
        print('Error: {}'.format(e))


def createTeamRankTable(conn):
    try:
        with open('team-rankings.csv') as file:
            dr = csv.DictReader(file)
            to_db = [(i['name'], i['rank'], i['lineup']) for i in dr]
        c = conn.cursor()
        createTableSql = """ CREATE TABLE IF NOT EXISTS teamRanking(
                                            name TEXT,
                                            rank FLOAT,
                                            lineup TEXT,
                                            UNIQUE (name)
                                        ); """
        c.execute(createTableSql)
        c.executemany("""INSERT OR IGNORE INTO teamRanking (name, rank, lineup) VALUES (?, ?, ?);""", to_db)
        conn.commit()
    except Error as e:
        print('Error: {}'.format(e))


def createPlayerTable(playerName, conn):
    try:
        with open('{}*player-stats.csv'.format(playerName)) as file:
            dr = csv.DictReader(file)
            to_db = [(i['matchDate'], i['rating'], i['matchId']) for i in dr]
        playerName = ''.join(e for e in playerName if e.isalnum())
        c = conn.cursor()
        createTableSql = """ CREATE TABLE IF NOT EXISTS {}(
                                            matchDate TEXT,
                                            rating FLOAT,
                                            matchId FLOAT,
                                            UNIQUE (matchId)
                                        ); """.format(playerName)
        c.execute(createTableSql)
        c.executemany("""INSERT OR IGNORE INTO {} (matchDate, rating, matchId) VALUES (?, ?, ?);""".format(playerName), to_db)
        conn.commit()
    except Error as e:
        print('Error: {}'.format(e))


def importCSVtoDB(conn):
    listOfCsv = glob.glob("*.csv")
    createTeamRankTable(conn)
    for file in listOfCsv:
        if 'match-stats' in file:
            file = file.split('*')
            teamName = file[0]
            createTeamTable(teamName, conn)
        elif 'player-stats' in file:
            file = file.split('*')
            playerName = file[0]
            createPlayerTable(playerName, conn)


def dropTables(conn):
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for table in c.fetchall():
        c.execute("DROP TABLE IF EXISTS {};".format(table[0]))
    conn.commit()


def main():
    database = "/Users/wt/Desktop/CSGO Stats Database/CSGOsqlite.db"
    conn = createConnection(database)
    while True:
        x = input("1. Import CSV to Database\n2. Drop All Tables\nq\n")
        if x == "1":
            importCSVtoDB(conn)
        elif x == '2':
            dropTables(conn)
        else:
            break;
    conn.close()


if __name__ == '__main__':
    main()

