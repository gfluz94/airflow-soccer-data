from typing import Tuple
import os
import requests
import re
import bs4
from tqdm import tqdm
from datetime import datetime
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date


STAGING_FOLDER = os.getenv("STAGING_AREA_FOLDER")
STAGING_FILEPATH = os.path.join(STAGING_FOLDER, "matches_results.csv")
DB_PATH = os.getenv("DB_FOLDER")
DB_NAME = "match_results.db"
WEB_ADDRESS_ORIGIN = "https://www.cbf.com.br/futebol-brasileiro/competicoes/campeonato-brasileiro-serie-a/"
HEADERS = {
    'User-Agent': \
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
}


def get_team_mapper(entry: bs4.element.Tag) -> str:
    team_name_mapper = {}
    teams_content = entry.find_all("div", "col-xs-4 nopadding p-t-5 p-b-5 bg-white")
    for team_content in teams_content:
        fullname = team_content.find("img")
        if fullname is not None:
            team_name_mapper[fullname.get("src")] = team_name_mapper.get(fullname.get("src"), fullname.get("alt"))
    return team_name_mapper


def get_match_results():
    base_address = WEB_ADDRESS_ORIGIN
    response = requests.get(f"{base_address}2021", headers=HEADERS)
    content = bs4.BeautifulSoup(response.content, 'html.parser')
    query_season = "\n".join([str(found) for found in content.find_all("option")])
    seasons = [datetime.now().year-i for i in range(4)]
    championship_seasons = [int(s) for s in re.findall(r"(\d{4})</option>", query_season) if int(s) in seasons]
    
    dfs = []
    
    print("Starting data extraction...")

    for year in tqdm(championship_seasons):
        print(f"\tScraping data of season {year}...")
        response = requests.get(f"{base_address}{year}", headers=HEADERS)
        content = bs4.BeautifulSoup(response.content, 'html.parser')
        games_results = {
            "date": [],
            "home": [],
            "away": [],
            "home_goals": [],
            "away_goals": [],
            "result": []
        }
        
        team_name_mapper = get_team_mapper(content)
        
        for team_games in content.find_all("chart-time"):
            all_games = eval(team_games.attrs[':data'])
            for game in all_games:
                date = "-".join(re.split(r"\\/", game["data"])[::-1])
                home_team_escudo = re.sub(r"\\", "", game["time1"]["escudo"])
                home_team = team_name_mapper[home_team_escudo]
                home_team_goals = game["time1"]["gols"]
                away_team_escudo = re.sub(r"\\", "", game["time2"]["escudo"])
                away_team = team_name_mapper[away_team_escudo]
                away_team_goals = game["time2"]["gols"]
                if home_team_goals > away_team_goals:
                    result = "H"
                elif home_team_goals < away_team_goals:
                    result = "A"
                else:
                    result = "D"
                games_results["date"].append(date)
                games_results["home"].append(home_team)
                games_results["away"].append(away_team)
                games_results["home_goals"].append(home_team_goals)
                games_results["away_goals"].append(away_team_goals)
                games_results["result"].append(result)

        print(f"\tFinished season {year}!", end="\n\n")

        df = pd.DataFrame(games_results) \
                .drop_duplicates() \
                .reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])
        dfs.append(df)

    output = pd.concat(dfs, axis=0)
    output.to_csv(STAGING_FILEPATH, index=False)
    print("Successfully finished data extraction stage!")




def persist_soccer_data():
    print("Starting loading stage...")
    results_df = pd.read_csv(STAGING_FILEPATH)
    results_df["date"] = pd.to_datetime(results_df["date"])

    engine = create_engine(f'sqlite:///{os.path.join(DB_PATH, DB_NAME)}', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base = declarative_base()

    class Match(Base):
        __tablename__ = 'match'

        id = Column(Integer, primary_key=True)
        date = Column(Date)
        home_team = Column(String)
        away_team = Column(String)
        home_goals = Column(Integer)
        away_goals = Column(Integer)
        result = Column(String)

        def __repr__(self):
            return f'Match {self.name}'

    if DB_NAME in os.listdir(DB_PATH):
        df = pd.read_sql('SELECT MAX(date) AS max_date FROM match', f'sqlite:///{os.path.join(DB_PATH, DB_NAME)}')
        max_date = df.iloc[0, 0]
        results_df = results_df.loc[results_df.date > max_date, :]
    else:
        Base.metadata.create_all(engine)

    for (_, row) in results_df.iterrows():
        date, home_team, away_team, home_goals, away_goals, result = row.values
        entry = Match(date=date, home_team=home_team, away_team=away_team, 
                    home_goals=home_goals, away_goals=away_goals, result=result)
        session.add(entry)
    session.commit()

    os.remove(STAGING_FILEPATH)
    print("Successfully finished loading stage!")