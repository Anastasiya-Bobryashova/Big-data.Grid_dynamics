from mrjob.step import MRStep
from mrjob.job import MRJob
from requests import get
from re import search
import requests

def get_location_by_IP(IP):
    Link =f'https://ip2c.org/?ip={IP}'
    response = requests.get(Link, timeout = (300, 400)).text
    location = response.split(';')[-1]
    return location

def format_result(data):
        result = dict(data)
        result = {int(hour): location_and_clicks for hour, location_and_clicks in result.items()} 
        result = sorted(result.items())
        result = {str(hour) + ':00': location_and_clicks for hour, location_and_clicks in result} 
        return result

class MR_time_location(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper = self.mapper_hour_with_IP,
                reducer = self.reducer
            ),
            MRStep(
                mapper = self.mapper_hour_with_location,
                reducer = self.reducer
            ),
            MRStep(
                mapper = self.mapper_hour_with_location_and_clicks,
                reducer = self.mapper_hour_with_location_and_clicks
            ),
            MRStep(
                mapper = self.mapper_result,
                reducer = self.reducer_result
            )
        ]
    
    def mapper_hour_with_IP(self,_, line):
        IP = search(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', line).group(1) 
        time = search(r'(\d{1,3}\:)', line).group(1)
        hour = int(time[:2])
        if 8 <= hour <= 18:
            yield (hour, IP), 1
            
    def mapper_hour_with_location(self, IP_and_hour, count):
        IP, hour = IP_and_hour
        location = get_location_by_IP(IP)
        yield (hour, location), count    
        
    def reducer(self, key, values):
        yield key, sum(values)

    def mapper_hour_with_location_and_clicks(self, location_and_hour, count):
        location, hour = location_and_hour
        yield hour, {location: count}

    def mapper_hour_with_location_and_clicks(self, hour, location_and_clicks):
        yield hour, list(location_and_clicks)

    def mapper_result(self, hour, location_and_clicks):
        yield None, (hour, location_and_clicks)

    def reducer_result(self,_, hour_and_location_with_clicks):
        result = format_result(hour_and_location_with_clicks) 
        yield None, result

if __name__ == '__main__':
     MR_time_location.run()