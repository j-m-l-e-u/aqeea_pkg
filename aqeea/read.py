import os
import pandas as pd
import polars as pl
from datetime import datetime
import glob
from aqeea.dict import pollutant_dict,source_dict


def historical(source=None, pollutant=None,datetime_from=None,datetime_to=None,lat_min=None,lat_max=None,lon_min=None,lon_max=None,blob_path=None):
    '''
    The function `historical` helps getting historical data from reference monitoring stations registered at EEA.
    Args:
        source:
        Source of the dataset; either validated (E1a), or up-to-date (E2a) but not validated by experts.

        pollutant:
        Pollutant of interest.

        datetime_from:
        Start of the period of interest in date_time format, i.e YYYY-MM-DD HH:MM:SS.

        datetime_to:
        End of the period of interest in date_time format, i.e YYYY-MM-DD HH:MM:SS.

        lat_min:
        Southern latitude describing the boundary box of interest

        lat_max:
        Nothern latitude describing the boundary box of interest

        lon_min:
        Eastern longitude describing the boundary box of interest

        lon_max:
        Western longitude describing the boundary box of interest

        blob_path:
        Path of the blob where to store the dataset.
        If a path is not provided, the cache folder created at the path where the code will be targeted.
    '''

    # Argument blob_path
    if blob_path is None:
        parent_dir = os.getcwd()
        blob_path = os.path.join(parent_dir,"cache")
    else:
        blob_path = blob_path

    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path. Please, download first the dataset.")

    res = read_historical(pollutant=pollutant,datetime_from=datetime_from,datetime_to=datetime_to,lat_min=lat_min,lat_max=lat_max,lon_min=lon_min,lon_max=lon_max,source=source, blob_path=blob_path)

    return res

def latest(pollutant=None,lat_min=None,lat_max=None,lon_min=None,lon_max=None,blob_path=None):
    '''
    The function `latest` helps getting the latest 48h data from reference monitoring stations registered at EEA.
    Args:
        pollutant:
        Pollutant of interest.

        lat_min:
        Southern latitude describing the boundary box of interest

        lat_max:
        Nothern latitude describing the boundary box of interest

        lon_min:
        Eastern longitude describing the boundary box of interest

        lon_max:
        Western longitude describing the boundary box of interest

        blob_path:
        Path of the blob where to store the dataset.
        If a path is not provided, the cache folder created at the path where the code will be targeted.

    '''

    # Argument blob_path
    if blob_path is None:
        parent_dir = os.getcwd()
        blob_path = os.path.join(parent_dir,"cache")
    else:
        blob_path = blob_path

    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path. Please, download first the dataset.")

    res = read_latest(pollutant=pollutant,lat_min=lat_min,lat_max=lat_max,lon_min=lon_min,lon_max=lon_max,blob_path=blob_path)

    return res

def metadata(pollutant=None,lat_min=None,lat_max=None,lon_min=None,lon_max=None,blob_path=None):
    '''
    The function `metadata` helps getting the metadata of the reference monitoring stations registered at EEA.
    Args:
        pollutant:
        Pollutant of interest.

        lat_min:
        Southern latitude describing the boundary box of interest

        lat_max:
        Nothern latitude describing the boundary box of interest

        lon_min:
        Eastern longitude describing the boundary box of interest

        lon_max:
        Western longitude describing the boundary box of interest

        blob_path:
        Path of the blob where to store the dataset.
        If a path is not provided, the cache folder created at the path where the code will be targeted.
    '''

    # Argument blob_path
    if blob_path is None:
        parent_dir = os.getcwd()
        blob_path = os.path.join(parent_dir,"cache")
    else:
        blob_path = blob_path

    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path. Please, download first the dataset.")

    res = read_metadata(pollutant=pollutant,lat_min=lat_min,lat_max=lat_max,lon_min=lon_min,lon_max=lon_max,blob_path=blob_path)

    return res

def read_historical(pollutant,datetime_from,datetime_to,lat_min,lat_max,lon_min,lon_max,source, blob_path):

    # dict to dataframe
    pollutant_pd = pd.DataFrame(pollutant_dict)
    source_pd = pd.DataFrame(source_dict)

    # Argument pollutant
    tmp = pollutant_pd.loc[pollutant_pd['pollutant']==pollutant,'shortpl']
    if len(tmp)>0:
        Pollutant = tmp.values[0]
    else:
        raise ValueError("The argument 'pollutant' does not refere to any pollutant")

    # Argument source
    tmp = source_pd.loc[source_pd['source']==source,'shorts']
    if len(tmp)>0:
        Source = tmp.values[0]
    else:
        raise ValueError("The argument 'source' does not refere to any source.")

    # Blob path
    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path.")

    meta_path = os.path.join(blob_path,'metadata')
    if source == "validated":
        data_path = os.path.join(blob_path,'validated')
    elif source == "uptodate":
        data_path = os.path.join(blob_path,'uptodate')

    # file spo_pol_id_file
    spo_pol_id_file = os.path.join(data_path,'spo_pol_id_file.csv')
    if not os.path.isfile(spo_pol_id_file):
        raise ValueError("The file 'spo_pol_id_file.csv' does not exit. Download first the data.")

    # read metadata
    meta = pl.read_csv(os.path.join(meta_path,'metadata.csv'),sep="\t",
            dtypes = {"Longitude": pl.Float64,
                      "Latitude": pl.Float64,
                      "Altitude": pl.Float64,
                      "InletHeight" : pl.Float64,
                      "BuildingDistance" : pl.Float64,
                      "KerbDistance": pl.Float64}, null_values=['-999','-9999'])

    air_polluttant_code = 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/'+Pollutant

    # filter using lat/lon and pollutant code
    meta_query = meta.filter(
        (pl.col("Latitude") >= lat_min) & (pl.col("Latitude") <= lat_max) & (pl.col("Longitude") >= lon_min) & (pl.col("Longitude") <= lon_max) & (pl.col("AirPollutantCode") == air_polluttant_code)
    )
    meta_query = meta_query.select(['SamplingPoint','Longitude','Latitude','Altitude','InletHeight','AirQualityStationType','AirQualityStationArea'])
    meta_query = meta_query.unique()

    # spo to look at
    spo_list = meta_query['SamplingPoint'].unique().to_numpy().tolist()

    # collecting from the id_file corresponding to spo
    spo_pol_id_file_pd = pd.read_csv(spo_pol_id_file)
    file_list = spo_pol_id_file_pd[spo_pol_id_file_pd['spo_pol_id'].isin(spo_list)]['filenames'].tolist()
    paths = [os.path.join(data_path, f) for f in file_list]

    queries = []
    for file in paths:
        q = pl.scan_csv(file)
        queries.append(q)

    tmp = pl.collect_all(queries)
    new_df = pl.concat(tmp, rechunk=True)
    new_df = new_df.with_columns(pl.col("DatetimeBegin").str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M:%S %z"))
    new_df = new_df.sort('DatetimeBegin')
    new_df = new_df.pivot(values = 'Concentration', index='DatetimeBegin', columns='SamplingPoint')

    # filter w/ date_from and date_to
    dt_from = datetime.fromisoformat(datetime_from)
    dt_to = datetime.fromisoformat(datetime_to)
    res = new_df.filter(
        pl.col('DatetimeBegin').is_between(dt_from,dt_to))

    return res

def read_latest(pollutant,lat_min,lat_max,lon_min,lon_max,blob_path):

    # dict to dataframe
    pollutant_pd = pd.DataFrame(pollutant_dict)

    # Argument pollutant
    tmp = pollutant_pd.loc[pollutant_pd['pollutant']==pollutant,'shortpl']
    if len(tmp)>0:
        Pollutant = tmp.values[0]
    else:
        raise ValueError("The argument 'pollutant' does not refere to any pollutant")

    # Blob path
    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path.")

    meta_path = os.path.join(blob_path,'metadata')

    data_path = os.path.join(blob_path,"latest")

    # read metadata
    meta = pl.read_csv(os.path.join(meta_path,'metadata.csv'),sep="\t",
            dtypes = {"Longitude": pl.Float64,
                      "Latitude": pl.Float64,
                      "Altitude": pl.Float64,
                      "InletHeight" : pl.Float64,
                      "BuildingDistance" : pl.Float64,
                      "KerbDistance": pl.Float64}, null_values=['-999','-9999'])

    air_polluttant_code = 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/'+Pollutant

    # filter using lat/lon and pollutant code
    meta_query = meta.filter(
        (pl.col("Latitude") >= lat_min) & (pl.col("Latitude") <= lat_max) & (pl.col("Longitude") >= lon_min) & (pl.col("Longitude") <= lon_max) & (pl.col("AirPollutantCode") == air_polluttant_code)
    )
    meta_query = meta_query.select(['SamplingPoint','Longitude','Latitude','Altitude','InletHeight','AirQualityStationType','AirQualityStationArea'])
    meta_query = meta_query.unique()

    # spo to look at
    spo_list = meta_query['SamplingPoint'].unique().to_numpy().tolist()

    # collecting data
    queries = []
    for file in glob.glob(os.path.join(data_path,'*'+'csv')):
        q = pl.scan_csv(file)
        queries.append(q)

    tmp = pl.collect_all(queries)
    new_df = pl.concat(tmp, rechunk=True)
    new_df = new_df.with_columns(pl.col("value_datetime_begin").str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M:%S %z"))
    new_df = new_df.sort('value_datetime_begin')

    # filter sampling points
    new_df = new_df.filter(
        pl.col('samplingpoint_localid').is_in(spo_list))

    # table datetime, spo...
    res = new_df.pivot(values = 'value_numeric', index='value_datetime_begin', columns='samplingpoint_localid')

    return res

def read_metadata(pollutant,lat_min,lat_max,lon_min,lon_max,blob_path):

    # dict to dataframe
    pollutant_pd = pd.DataFrame(pollutant_dict)

    # Argument pollutant
    tmp = pollutant_pd.loc[pollutant_pd['pollutant']==pollutant,'shortpl']
    if len(tmp)>0:
        Pollutant = tmp.values[0]
    else:
        raise ValueError("The argument 'pollutant' does not refere to any pollutant")

    # Blob path
    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path.")

    meta_path = os.path.join(blob_path,'metadata')

    # read metadata
    meta = pl.read_csv(os.path.join(meta_path,'metadata.csv'),sep="\t",
            dtypes = {"Longitude": pl.Float64,
                      "Latitude": pl.Float64,
                      "Altitude": pl.Float64,
                      "InletHeight" : pl.Float64,
                      "BuildingDistance" : pl.Float64,
                      "KerbDistance": pl.Float64}, null_values=['-999','-9999'])

    air_polluttant_code = 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/'+Pollutant

    # filter using lat/lon and pollutant code
    meta_query = meta.filter(
        (pl.col("Latitude") >= lat_min) & (pl.col("Latitude") <= lat_max) & (pl.col("Longitude") >= lon_min) & (pl.col("Longitude") <= lon_max) & (pl.col("AirPollutantCode") == air_polluttant_code)
    )
    res = meta_query.unique()

    return res
