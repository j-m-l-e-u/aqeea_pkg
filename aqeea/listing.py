import pandas as pd
from aqeea.dict import country_pollutant_dict,country_city_dict

def pollutant_by_country(country:str):
    '''
    The function `pollutant_by_country` gives the pollutants measured in the country by the reference monitoring stations registered at EEA.

    Args:
        country:
        Country of the monitoring stations.

    '''

    country_pollutant_pd = pd.DataFrame(country_pollutant_dict)
    res = country_pollutant_pd[country_pollutant_pd.index==country].values[0][0]
    return res


def city_by_country(country:str):
    '''
    The function `city_by_country` gives the cities in the country whith measurements using the reference monitoring stations registered at EEA.

    Args:
        country:
        Country of the monitoring stations.

    '''

    country_city_pd = pd.DataFrame(country_city_dict)
    res = country_city_pd[country_city_pd.index==country].values[0][0]
    return res
