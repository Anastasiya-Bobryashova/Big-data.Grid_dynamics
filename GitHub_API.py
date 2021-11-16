'''
Using the GitHub API for organizations for the first 200 organizations, you need to calculate the TOP 20 most "stellar" repositories.
The resulting TOP must be saved to the database using SQLAlchemy.
'''

from sqlalchemy import Column, Integer, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from operator import itemgetter
from numpy import genfromtxt
import pandas as pd
import requests
import json
import csv

token = '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'
headers = {'Accept':'application/vnd.github+json', 'authorization': f'Bearer {token}'}
max_id = 0

class Fetch():

    def get_organizations(self):
        list_of_organizations = []
        url = 'https://api.github.com/organizations?per_page=100'
    
        for i in range(2):
            global max_id
            parameter = {'since':max_id}
            response = requests.get(url, params = parameter, headers = headers)
            response_dict = response.json()
            list_of_organizations.extend([organization['login'] for organization in response_dict])
            max_id= max([organization['id'] for organization in response_dict ])+1
        return list_of_organizations
    
    def get_repositories(self, organizations):
        list_of_repositories =[]
        for organization in organizations:
            link = 'https://api.github.com/orgs/'+ organization +'/repos'
            link_to_the_repositories = requests.get(link, headers = headers).json()
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
    org_name = Column(Text)
    repo_name = Column(Text)
    stars_count = Column(Integer)

data = Fetch()
organizations = data.get_organizations()
repos = data.get_repositories(organizations)
top_rep = data.top_repositories(repos)

with open('Top_repositories.csv', 'w', newline = '') as Top:
    writer = csv.writer(Top)
    writer.writerow(['Organization name', 'Repository name', 'Number of stars'])
    for line in top_rep:
        writer.writerow(line)
    print('Writing complete')

engine = create_engine('sqlite:///cdb.db')
Base.metadata.create_all(engine)
file = 'Top_repositories.csv'
SQL_table = pd.read_csv(file)
SQL_table.to_sql(con = engine, index_label = 'id', name = Table.__tablename__, if_exists = 'replace')

print(SQL_table)