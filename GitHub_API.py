'''
Using the GitHub API for organizations for the first 200 organizations, you need to calculate the TOP 20 most "stellar" repositories.
The resulting TOP must be saved to the database using SQLAlchemy.
* Takes 30 organizations, edit
'''

from sqlalchemy import Column, Integer, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from operator import itemgetter
from numpy import genfromtxt
import pandas as pd
import requests
import csv

class Fetch():

    def get_organizations(self):
        list_of_organizations = []
        url = 'https://api.github.com/organizations'
        response = requests.get(url)
        response_dict = response.json()
        list_of_organizations.extend([organization['login'] for organization in response_dict])
        return list_of_organizations
    
    def get_repositories(self, organizations):
        list_of_repositories =[]
        for organization in organizations:
            link = 'https://api.github.com/orgs/'+ organization +'/repos'
            link_to_the_repositories = requests.get(link).json()
            list_of_repositories.extend( [[repository['owner']['login'], repository['name'], repository['stargazers_count']] for repository in link_to_the_repositories ])
        return list_of_repositories

    def top_repositories(self, repositories):
        r = repositories
        r = r.sort(key=itemgetter(2), reverse=True)
        r = repositories[:20]
        return r

Base = declarative_base() 
class Table(Base):
    __tablename__ = 'Top'
    id = Column(Integer, primary_key = True, nullable = False) 
    topd = Column(Integer)
    org_name = Column(Text)
    repo_name = Column(Text)
    stars_count = Column(Integer)

with open('Top_repositories.csv', 'w', newline = '') as Top:
    writer = csv.writer(Top)
    writer.writerow(['Organization name', 'Repository name', 'Number of stars'])
    for line in topic:
        writer.writerow(line)
    print("Writing complete")

engine = create_engine('sqlite:///cdb.db')
Base.metadata.create_all(engine)
file_name = 'Top_repositories.csv'
SQL_table = pd.read_csv(file_name)
SQL_table.to_sql(con = engine, index_label = 'id', name = Table.__tablename__, if_exists = 'replace')

data = Fetch()
organizations = data.get_organizations()
repos = data.get_repositories(organizations)
topic = data.top_repositories(repos)

print(SQL_table)
