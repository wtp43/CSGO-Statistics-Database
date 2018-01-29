import sqlite3
from sqlite3 import Error
import csv
import glob
import pandas as pd


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
            to_db = [('20' + i['matchDate'], i['event'], i['map'].lower(), teamName.lower(), i['score'],i['opponent'].lower(),i['opponentScore'],i['result'],
                      i['type'], i['matchId']) for i in dr]
        teamName = ''.join(e for e in teamName if e.isalnum())
        createTableSql = """ CREATE TABLE IF NOT EXISTS {} (
                                            matchDate DATE ,
                                            event TEXT,
                                            map TEXT COLLATE NOCASE,
                                            team1 TEXT COLLATE NOCASE,
                                            score1 INTEGER,
                                            team2 TEXT COLLATE NOCASE,
                                            score2 INTEGER,
                                            result TEXT,
                                            type TEXT,
                                            matchId FLOAT,
                                            UNIQUE (matchId)
                                        );""".format(teamName)
        c = conn.cursor()
        c.execute(createTableSql)
        c.executemany(""" INSERT OR IGNORE INTO {} (
                                matchDate, event, map, team1, score1, 
                                team2, score2, result, type, matchId) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""".format(teamName), to_db)
        conn.commit()
    except Error as e:
        print('Error: {}'.format(e))


def createTeamRankTable(conn):
    try:
        with open('team-rankings.csv') as file:
            dr = csv.DictReader(file)
            to_db = [(''.join(e for e in i['name'] if e.isalnum()), i['rank'], i['lineup']) for i in dr]
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
            to_db = [('20'+i['matchDate'], i['rating'], i['matchId']) for i in dr]
        playerName = ''.join(e for e in playerName if e.isalnum())
        c = conn.cursor()
        createTableSql = """ CREATE TABLE IF NOT EXISTS {}(
                                            matchDate DATE,
                                            rating FLOAT,
                                            matchId INTEGER,
                                            UNIQUE (matchId)
                                        ); """.format(playerName)
        c.execute(createTableSql)
        c.executemany("""INSERT OR IGNORE INTO {} (matchDate, rating, matchId) VALUES (?, ?, ?);""".format(playerName), to_db)
        conn.commit()
    except Error as e:
        print('Error: {}'.format(e))






def hth_win_percentage(team1,team2, c):
    for map in mapList:
        sql = """SELECT result FROM {} """.format(playerName, numMatches)
        c.execute(sql)


def hth_win_percentage_diff(team1, team2, c):
    for map in maplist:
        sql = """SELECT FROM {} ORDER BY matchDate DESC LIMIT '{}'""".format(playerName, numMatches)
        c.execute(sql)




#def map_round_diff(team1, team2, map, c):



def map_win_percentage(teamName, map, c, matchDate):
    c.execute("""SELECT result FROM {} 
                WHERE result = 'L' 
                AND matchDate < '{}' 
                AND map == '{}';"""
              .format(teamName, matchDate, map))
    losses = c.fetchall()
    numLosses = len(losses)
    c.execute("""SELECT result FROM {} WHERE result = 'W' AND matchDate < '{}' AND map == '{}';"""
              .format(teamName, matchDate, map))
    wins = c.fetchall()
    numWins = len(wins)
    c.execute("""SELECT result FROM {} WHERE result = 'T' AND matchDate < '{}' AND map == '{}';"""
              .format(teamName, matchDate, map))
    ties = c.fetchall()
    numTies = len(ties)
    if (numWins == numTies == numLosses == 0):
        percentage = 0
    else:
        percentage = numWins / (numLosses + numWins + numTies)
    return percentage

def map_win_percentage_diff(team1, team2, map, c, matchDate):
    team1Diff = map_win_percentage(team1, map, c, matchDate)
    team2Diff = map_win_percentage(team2, map, c, matchDate)
    # if (team1Diff == None or team2Diff == None):
    #     return 'N/A'
    # else:
    return team1Diff - team2Diff



def avg_player_rating(playerName, numMatches, c, matchDate):
    sql = """SELECT rating FROM {} 
                WHERE matchDate < '{}' ORDER BY matchDate DESC LIMIT {} ;""".format(playerName, matchDate, numMatches)
    c.execute(sql)
    total = 0
    for rating in c.fetchall():
        total += rating[0]
    return total/numMatches


def team_avg_rating(teamName, numMatches, c, matchDate):
    totalRating = 0
    sql = """SELECT lineup FROM teamRanking WHERE name = '{}'; """.format(teamName)
    c.execute(sql)
    lineup = c.fetchall()[0][0]
    lineup = lineup.strip('[]{}').split(',')
    for player in lineup:
        playerName = player.split(':')[0]
        playerName = ''.join(e for e in playerName if e.isalnum())
        totalRating += avg_player_rating(playerName, numMatches, c, matchDate)
    return totalRating/len(lineup)


def getAvgRatingDiff(team1, team2, numMatches, c, matchDate):
    return team_avg_rating(team1, numMatches, c, matchDate) - team_avg_rating(team2, numMatches, c, matchDate)


def getScoreDiff(match):
    return match[4] - match[6]
def createAllMatchesTable(conn):
    createTableSql = """ CREATE TABLE IF NOT EXISTS allMatches (
                                            matchDate DATE,
                                            event TEXT,
                                            map TEXT,
                                            team1 TEXT,
                                            score1 INTEGER,
                                            team2 TEXT,
                                            score2 INTEGER,
                                            result TEXT,
                                            type TEXT,
                                            matchId FLOAT,
                                            scoreDiff FLOAT,
                                            nukeDiff FLOAT,
                                            mirageDiff FLOAT,
                                            trainDiff FLOAT,
                                            cacheDiff FLOAT,
                                            overpassDiff FLOAT,
                                            cobblestoneDiff FLOAT,
                                            infernoDiff FLOAT,
                                            ratingDiff FLOAT,                      
                                            UNIQUE (matchId)
                                        ); """
    c = conn.cursor()
    c.execute(createTableSql)
    c.execute("SELECT name FROM teamRanking")
    for table in c.fetchall():
        sql = """SELECT matchDate, event, map, team1, score1, team2, score2,  result, type, matchId FROM {} 
        WHERE matchId NOT IN (SELECT matchId FROM allMatches) 
        AND team1 IN (SELECT name FROM teamRanking) 
        AND team2 IN (SELECT name FROM teamRanking);""".format(table[0])
        print(table[0])
        c.execute(sql)
        listMatches = c.fetchall()
        for match in listMatches:
            nukeDiff = map_win_percentage_diff(match[3], match[5], 'nuke', c, match[0])
            mirageDiff = map_win_percentage_diff(match[3], match[5], 'mirage', c, match[0])
            cacheDiff = map_win_percentage_diff(match[3], match[5], 'cache', c, match[0])
            trainDiff = map_win_percentage_diff(match[3], match[5], 'train', c, match[0])
            overpassDiff = map_win_percentage_diff(match[3], match[5], 'overpass', c, match[0])
            cobblestoneDiff = map_win_percentage_diff(match[3], match[5], 'cobblestone', c, match[0])
            infernoDiff = map_win_percentage_diff(match[3], match[5], 'inferno', c, match[0])
            ratingDiff = getAvgRatingDiff(match[3], match[5], 15, c, match[0])
            scoreDiff = getScoreDiff(match)
            completeMatchInfo = match + (scoreDiff, nukeDiff, mirageDiff, trainDiff, cacheDiff, overpassDiff, cobblestoneDiff, infernoDiff, ratingDiff)
            c.execute("""INSERT INTO allMatches (matchDate ,
                                            event, map, team1 , score1 , team2 , score2 ,result ,
                                            type , matchId , scoreDiff, nukeDiff , mirageDiff , trainDiff ,
                                            cacheDiff ,overpassDiff , cobblestoneDiff, infernoDiff,
                                            ratingDiff) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",completeMatchInfo)



    conn.commit()

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
    try:
        database = "/Users/wt/Desktop/CSGO Stats Database/CSGOsqlite.db"
        conn = createConnection(database)
        while True:
            x = input("1. Import CSV to Database\n2. Drop All Tables\nq\n")
            if x == "1":
                importCSVtoDB(conn)
                createAllMatchesTable(conn)
            elif x == '2':
                dropTables(conn)
            else:
                break;
    except KeyboardInterrupt:
        print("End process by keyboard interruption")
    finally:
        conn.close()


if __name__ == '__main__':
    main()

