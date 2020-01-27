#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 26 13:58:51 2020

@author: aclark
"""

import psycopg2
import pandas as pd
import numpy as np

def Hitter(start_date, end_date, season="2019", min_pa=100):

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # connect to the database
    pl_host = os.getenv('PL_DB_HOST')
    pl_db = 'pitcher-list'
    pl_user = os.getenv('PL_DB_USER')
    pl_password = os.getenv('PL_DB_PW')
    con = psycopg2.connect(host=pl_host, port=5432, dbname=pl_db, user=pl_user, password=pl_password)

    ### GAMES
    cursor = con.cursor()
    cursor.execute("select * from game_detail")
    colnames = [i[0] for i in cursor.description]

    games = cursor.fetchall()
    games = pd.DataFrame(np.array(games), columns=colnames).infer_objects()
    games = games.assign(date = pd.to_datetime(games['date']))
    games = games.query('date > @start_year and postseason == False')

    ### PITCHES
    cursor = con.cursor()
    cursor.execute("select * from pitches")
    colnames = [i[0] for i in cursor.description]

    pitches = cursor.fetchall()
    pitches = pd.DataFrame(np.array(pitches), columns=colnames).infer_objects()
    pitches = pitches.assign(Date = pd.to_datetime(pitches['ghuid'].str[0:10]))
    pitches = pitches.query("Date >= @start_season & Date <= @end_season")

    ### BAT
    cursor = con.cursor()
    cursor.execute("select * from actions")
    colnames = [i[0] for i in cursor.description]

    bat = cursor.fetchall()
    bat = pd.DataFrame(np.array(bat), columns=colnames).infer_objects()
    bat = bat.assign(Date = pd.to_datetime(bat['ghuid'].str[0:10]))
    bat = bat.query("Date >= @start_season & Date <= @end_season")
    url = 'https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv'
    chadwick_player_lu_table = pd.read_csv(url)
    chadwick_player_lu_table = chadwick_player_lu_table[['name_first','name_last','key_mlbam']]
    bat = bat.merge(chadwick_player_lu_table, left_on='hittermlbamid',right_on ='key_mlbam', how='left')
    bat = bat[bat['ghuid'].isin(games['ghuid'])]
    bat['hittername'] = bat.name_first.map(str) +" "+ bat.name_last

    name_lookup = bat[["hittermlbamid","hittername"]]

    ### RUNS
    cursor = con.cursor()
    cursor.execute("select * from runs")
    colnames = [i[0] for i in cursor.description]

    runs = cursor.fetchall()
    runs = pd.DataFrame(np.array(runs), columns=colnames).infer_objects()
    runs = runs[runs['ghuid'].isin(games['ghuid'])]

    ### BASERUNNING
    cursor = con.cursor()
    cursor.execute("select * from base_runners")
    colnames = [i[0] for i in cursor.description]

    baserunning = cursor.fetchall()
    baserunning = pd.DataFrame(np.array(baserunning), columns=colnames).infer_objects()
    baserunning = baserunning[baserunning['ghuid'].isin(games['ghuid'])]

    # close connection
    cursor.close()
    con.close()

    bs = bat.query("Date >= @start_date & Date <= @end_date")

    boxscore1 = bs.groupby("hittermlbamid")["primaryevent"].agg(
             [("X1B", (lambda x: (x=="S").sum())),
             ("X2B", (lambda x: (x=="D").sum())),
             ("X3B", (lambda x: (x=="T").sum())),
             ("HR", (lambda x: (x=="HR").sum())),
             ("uBB", (lambda x: (x=="BB").sum())),
             ("IBB", (lambda x: (x=="IBB").sum())),
             ("HBP", (lambda x: (x=="HBP").sum())),
             ("SF", (lambda x: ((x=="SF") | (x=="SH")).sum())),
             ("SO", (lambda x: ((x=="K") | (x=="KC") | (x=="KS")).sum()))])
    boxscore1 = boxscore1.assign(
             H = boxscore1.X1B + boxscore1.X2B + boxscore1.X3B + boxscore1.HR)

    boxscore2 = bs.groupby("hittermlbamid")["rbi"].agg(
            [("RBI", np.sum)])

    boxscore3 = boxscore1.merge(boxscore2, on="hittermlbamid", how='inner').reset_index(level=0)

    # Calculate the PAs per player
    PAs1 = bs.assign(pa_thisgame = bs.pa_thisgame.fillna(0)).groupby(
            ["ghuid", "Date", "hittermlbamid"]).agg(
                    PA = ("pa_thisgame", "max"),
                    G = ("ghuid", "nunique")).reset_index(level=[0, 1, 2])

    PAs = PAs1[['hittermlbamid', 'ghuid', 'Date', 'PA', 'G']].groupby("hittermlbamid").agg(
            PA = ("PA", "sum"),
            G = ("G", "sum")).reset_index(level=0)

    boxscore4 = boxscore3.merge(PAs, on="hittermlbamid", how='left')

    # calculate how many runs each runner scored
    runs1 = runs.assign(Date = pd.to_datetime(runs.ghuid.str.slice(0, 10))).query(
            "Date >= @start_date & Date <= @end_date").groupby("runnermlbamid").size().reset_index(
                            level=0).rename(columns={0: 'R'}).fillna(0)

    names1 = list(boxscore4.columns.values)
    names2 = [i for i in names1 if i not in ("hittermlbamid", "PA")]
    #boxscore5 = boxscore4[["hittermlbamid", "PA"] + names2].query("PA >= @min_pa")

    boxscore6 = pd.merge(boxscore4, runs1, how='left',
                         left_on=['hittermlbamid'], right_on=['runnermlbamid']).drop(
                                 'runnermlbamid', 1)

    # Calculate number of times caught stealing
    CS = baserunning.assign(Date = pd.to_datetime(baserunning.ghuid.str.slice(0, 10))).query(
            "Date >= @start_date & Date <= @end_date & csidmlbamid.notnull()",
            engine = 'python').groupby("csidmlbamid").size().reset_index(
                    level=0).rename(columns={0: "CS", "csidmlbamid": "hittermlbamid"}).fillna(0)

    # calculate stolen bases
    SB = baserunning.assign(Date = pd.to_datetime(baserunning.ghuid.str.slice(0, 10))).query(
            "Date >= @start_date & Date <= @end_date & sbidmlbamid.notnull()",
            engine = 'python').groupby("sbidmlbamid").size().reset_index(
                    level=0).rename(columns={0: "SB", "sbidmlbamid": "hittermlbamid"}).fillna(0)

    boxscore7 = boxscore6.merge(
            SB, on="hittermlbamid", how='left').merge(
                    CS, on="hittermlbamid", how='left')
    boxscore7[['PA', 'RBI', 'R']] = boxscore7[['PA', 'RBI', 'R']].apply(
            np.int64)
    qry = """ AB = (PA - (uBB + IBB + HBP + SF))
           AVG = H/AB
           OBP = (H + uBB + IBB + HBP)/(AB + uBB + IBB + HBP + SF)
           BABIP = (H - HR)/(AB - SO - HR + SF) """
    boxscore8 = boxscore7.eval(qry)
    boxscore8 = boxscore8.assign(
            BABIP = round(boxscore8.BABIP, 3),
            season = season)

    boxscore = boxscore8.merge(name_lookup, on = "hittermlbamid", how = 'left').drop_duplicates().fillna(0)
    cols_to_move = ['hittermlbamid', 'hittername', 'season', 'PA','G' ]
    boxscore = boxscore[ cols_to_move + [ col for col in boxscore.columns if col not in cols_to_move ]]

    return boxscore
