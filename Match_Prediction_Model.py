import pandas as pd
#import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn import svm
from sklearn.preprocessing import scale
from CSGO_Stats_Database_Generator import createConnection
from sklearn.model_selection import train_test_split
from time import time
from sklearn.metrics import accuracy_score

mapList = ['inferno', 'nuke', 'train', 'mirage', 'cache', 'overpass', 'cobblestone']

def convert_categorical_variates(data):
    newData = pd.DataFrame(index = data.index)

    for col, val in data.iteritems():
        if val.dtype == object:
            val = pd.get_dummies(val, prefix = col)
        newData = newData.join(val)
    return newData

import numpy as np

def main():
    try:
        cleanup_nums = {"result": {"W": 1, "L": -1, "T":0}}
        database = "/Users/wt/Desktop/CSGO Stats Database/CSGOsqlite.db"
        conn = createConnection(database)
        c = conn.cursor()
        data = pd.read_sql_query("""SELECT result, nukeDiff, mirageDiff,
                                 trainDiff, cacheDiff, overpassDiff, cobblestoneDiff, ratingDiff
                                 FROM allMatches ;""", conn)

        data.replace(cleanup_nums, inplace=True)
        X = data.drop(['result'],1)
        y = data['result']
        cols_to_scale = ['nukeDiff', 'mirageDiff', 'trainDiff', 'cacheDiff', 'cobblestoneDiff',
                         'ratingDiff']
        for col in cols_to_scale:
            X[col] = scale(X[col])
        X = convert_categorical_variates(X)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, stratify=y)
        s = svm.SVC(random_state=1, C=1, kernel='rbf')
        model = s.fit(X_train, y_train)
        #predictions = s.predict(X_test)
        print("SVM SCORE:", model.score(X_test, y_test))
        lrm = LogisticRegression(random_state=1)
        model = lrm.fit(X_train, y_train)
        #predictions = lrm.predict(X_test)
        print("Linear regression SCORE:", model.score(X_train, y_train))
    except KeyboardInterrupt:
        print("Process ended by keyboard interruption")
    finally:
        conn.close()


if __name__ == '__main__':
    main()