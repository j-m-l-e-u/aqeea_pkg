import os
import pandas as pd
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import time
import requests
from aqeea.dict import country_dict,pollutant_dict,source_year_dict,source_dict



def d_historical(source:str,COUNTRIES=False, POLLUTANTS=False, YEARS=False,  country=None, pollutant=None, year_from=2013, year_to=2013, blob_path=None) -> bool:
    '''
    The function `d_historical` helps retrieving historical data from reference monitoring stations registered at EEA.
    Args:
        source:
        Source of the dataset; either validated (E1a), or up-to-date (E2a) but not validated by experts.

        COUNTRIES: boolean
        If `True`, all the countries registered at EEA are selected.
        If `False`, only the country given by the argument `country` is selected.

        POLLUTANTS: boolean
        If `True`, all the pollutants registered at EEA are selected.
        If `False`, only the pollutant given by the argument `pollutant` is selected.

        YEARS: boolean
        If `True`, the whole period covering either the `validated` or the `up-to-date` data source.
        If `False`, only the period given by the lower and upper dates `year_from` and `year_to`

        country:
        Country of the monitoring stations.

        pollutant:
        Pollutant of interest.

        year_from:
        Start of the period of interest.

        year_to:
        End of the period of interest.

        blob_path:
        Path of the blob where to store the dataset.
        If a path is not provided, a folder will be created at the path where the code is running. In this case, the path will be printed.
    '''
    # dict to dataframe
    country_pd = pd.DataFrame(country_dict)
    pollutant_pd = pd.DataFrame(pollutant_dict)
    source_year_pd = pd.DataFrame(source_year_dict)
    source_pd = pd.DataFrame(source_dict)

    # argument COUNTRIES
    if isinstance(COUNTRIES, bool) and COUNTRIES:
        country_list = country_pd['country'].tolist()
    elif isinstance(COUNTRIES, bool) and not COUNTRIES:
        if country is not None:
            country_list = [country]
        else:
            raise ValueError("The argument 'country' is None and 'COUNTRY' is False.")
    else:
        raise ValueError("The argument 'COUNTRY' is not a boolean.")

    # Argument POLLUTANTS
    if isinstance(POLLUTANTS, bool) and POLLUTANTS:
        pollutant_list = pollutant_pd['pollutant'].tolist()
    elif isinstance(POLLUTANTS, bool) and not POLLUTANTS:
        if pollutant is not None:
            pollutant_list = [pollutant]
        else:
            raise ValueError("The argument 'pollutant' is None and 'POLLUTANT' is False.")
    else:
        raise ValueError("The argument 'POLLUTANT' is not a boolean.")

    # Argument source
    tmp = source_pd.loc[source_pd['source']==source,'shorts']
    if len(tmp)>0:
        Source = tmp.values[0]
    else:
        raise ValueError("The argument 'source' does not refere to any source.")

    # Arguments years
    if isinstance(YEARS, bool) and YEARS:
        if source=='validated':
            year_from = source_year_pd.at[0,'year_start']
            year_to = source_year_pd.at[0,'year_end']
        elif source=='uptodate':
            year_from = source_year_pd.at[1,'year_start']
            year_to = source_year_pd.at[1,'year_end']
        else:
            raise ValueError("The argument 'source' does not refere to any source.")
    elif isinstance(YEARS, bool) and not YEARS:
        if isinstance(year_from, int) and isinstance(year_to, int):
            year_from = year_from
            year_to = year_to
        else:
            raise ValueError("The arguments 'year_from' and/or 'year_to' are not integers and 'YEARS' is False.")
    else:
        raise ValueError("The argument 'YEARS' is not a boolean.")

    # Argument blob_path
    if blob_path is None:
        parent_dir = os.getcwd()
        blob_path = os.path.join(parent_dir,"cache")
        os.makedirs(blob_path, exist_ok=True)
        print('blob_path is {}'.format(blob_path))
    elif os.path.exists(blob_path):
        blob_path = blob_path
    else:
        raise ValueError("The argument 'blob_path' targets a non-exising path.")

    #download
    for c in country_list:
        print('>> {}'.format(c))
        for p in pollutant_list:
            print('> {}'.format(p))
            download_historical(country=c,pollutant=p,year_from=year_from,year_to=year_to,source=source,blob_path=blob_path)

    return True

def d_latest(COUNTRIES=False, POLLUTANTS=False, country=None, pollutant=None, blob_path=None) -> bool:
    '''
    The function `d_latest` helps retrieving the latest 48h data from reference monitoring stations registered at EEA.
    Args:
        COUNTRIES: boolean
        If `True`, all the countries registered at EEA are selected.
        If `False`, only the country given by the argument `country` is selected.

        POLLUTANTS: boolean
        If `True`, all the pollutants registered at EEA are selected.
        If `False`, only the pollutant given by the argument `pollutant` is selected.

        country:
        Country of the monitoring stations.

        pollutant:
        Pollutant of interest.

        blob_path:
        Path of the blob where to store the dataset.
        If a path is not provided, a folder will be created at the path where the code is running. In this case, the path will be printed.
    '''
    # dict to dataframe
    country_pd = pd.DataFrame(country_dict)
    pollutant_pd = pd.DataFrame(pollutant_dict)

    # argument COUNTRIES
    if isinstance(COUNTRIES, bool) and COUNTRIES:
        country_list = country_pd['country'].tolist()
    elif isinstance(COUNTRIES, bool) and not COUNTRIES:
        if country is not None:
            country_list = [country]
        else:
            raise ValueError("The argument 'country' is None and 'COUNTRY' is False.")
    else:
        raise ValueError("The argument 'COUNTRY' is not a boolean.")

    # Argument POLLUTANTS
    if isinstance(POLLUTANTS, bool) and POLLUTANTS:
        pollutant_list = pollutant_pd['pollutant'].tolist()
    elif isinstance(POLLUTANTS, bool) and not POLLUTANTS:
        if pollutant is not None:
            pollutant_list = [pollutant]
        else:
            raise ValueError("The argument 'pollutant' is None and 'POLLUTANT' is False.")
    else:
        raise ValueError("The argument 'POLLUTANT' is not a boolean.")

    # Argument blob_path
    if blob_path is None:
        parent_dir = os.getcwd()
        blob_path = os.path.join(parent_dir,"cache")
        os.makedirs(blob_path, exist_ok=True)
        print('blob_path is {}'.format(blob_path))
    elif os.path.exists(blob_path):
        blob_path = blob_path
    else:
        raise ValueError("The argument 'blob_path' targets a non-exising path.")

    #download
    for c in country_list:
        print('>> {}'.format(c))
        for p in pollutant_list:
            print('> {}'.format(p))
            download_latest(country=c,pollutant=p,blob_path=blob_path)

    return True


def d_metadata(blob_path=None) -> bool:
    '''
    The function `d_metadata` helps retrieving the metadata of the reference monitoring stations registered at EEA.

    Args:
        blob_path:
        Path of the blob where to store the dataset.
        If a path is not provided, a folder will be created at the path where the code is running. In this case, the path will be printed.
    '''

    # Argument blob_path
    if blob_path is None:
        parent_dir = os.getcwd()
        blob_path = os.path.join(parent_dir,"cache")
        os.makedirs(blob_path, exist_ok=True)
        print('blob_path is {}'.format(blob_path))
    elif os.path.exists(blob_path):
        blob_path = blob_path
    else:
        raise ValueError("The argument 'blob_path' targets a non-exising path.")

    # download
    download_metadata(blob_path=blob_path)

    return True


def download_historical(country:str,pollutant:str,year_from:int,year_to:int,source:str,blob_path) -> bool:
    '''
    The function `download_historical` helps retrieving historical data from reference monitoring stations registered at EEA.

    Args:
        country:
        Country of the monitoring stations.

        pollutant:
        Pollutant of interest.

        year_from:
        Start of the period of interest.

        year_to:
        End of the period of interest.

        source:
        Source of the dataset; either validated (E1a), or up-to-date (E2a) but not validated by experts.

        blob_path:
        Path of the blob where to store the dataset.
    '''
    # constant urls
    url_root_download_service = "https://ereporting.blob.core.windows.net/downloadservice/"
    url_root ='https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw'

    # dict to dataframe
    country_pd = pd.DataFrame(country_dict)
    pollutant_pd = pd.DataFrame(pollutant_dict)
    source_year_pd = pd.DataFrame(source_year_dict)
    source_pd = pd.DataFrame(source_dict)

    # Argument country for the url
    tmp = country_pd.loc[country_pd['country']==country,'shortct']
    if len(tmp)>0:
        CountryCode = tmp.values[0]
    else:
        raise ValueError("The argument 'country' does not refere to any country.")

    # Argument pollutant for the url
    tmp = pollutant_pd.loc[pollutant_pd['pollutant']==pollutant,'shortpl']
    if len(tmp)>0:
        Pollutant = tmp.values[0]
    else:
        raise ValueError("The argument 'pollutant' does not refere to any pollutant")

    # Argument source for the url
    tmp = source_pd.loc[source_pd['source']==source,'shorts']
    if len(tmp)>0:
        Source = tmp.values[0]
    else:
        raise ValueError("The argument 'source' does not refere to any source.")

    # Argument year_from for the url
    lower = source_year_pd.loc[source_year_pd['source']==source,'year_start']
    upper = source_year_pd.loc[source_year_pd['source']==source,'year_end']

    if ((year_from>=lower.values[0]) & (year_from<=upper.values[0])):
        Year_from = str(year_from)
    else:
        raise ValueError("The argument 'year_from' is outsite the boundaries.")

    # Argument year_from for the url
    if ((year_to>=lower.values[0]) & (year_to<=upper.values[0])):
        Year_to = str(year_to)
    else:
        raise ValueError("The argument 'year_from' is outsite the boundaries.")

    if (year_from > year_to ):
        raise ValueError("The argument 'year_from' is higher than 'year_to'.")

    # Blob path
    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path.")
    else:
            if source == 'validated':
                blob_path_ = os.path.join(blob_path,"validated")
            elif source == 'uptodate':
                blob_path_ = os.path.join(blob_path,"uptodate")
            else:
                raise ValueError("The argument 'source' does not refere to any source.")
            os.makedirs(blob_path_, exist_ok=True)

    # Hard coded arguments for url
    CityName=''
    Station=''
    Samplingpoint=''
    Output='CSV'
    UpdateDate=''
    TimeCoverage='Year'

    # Make the urls
    url = f"{url_root}" \
    f"?CountryCode={CountryCode}" \
    f"&CityName={CityName}" \
    f"&Pollutant={Pollutant}" \
    f"&Year_from={Year_from}" \
    f"&Year_to={Year_to}" \
    f"&Station={Station}" \
    f"&Samplingpoint={Samplingpoint}" \
    f"&Source={Source}" \
    f"&Output={Output}" \
    f"&UpdateDate={UpdateDate}" \
    f"&TimeCoverage={TimeCoverage}"

    # Make the listing file
    listing_filename = f"listing" \
    f"_CountryCode={CountryCode}" \
    f"_CityName={CityName}" \
    f"_Pollutant={Pollutant}" \
    f"_Year_from={Year_from}" \
    f"_Year_to={Year_to}" \
    f"_Station={Station}" \
    f"_Samplingpoint={Samplingpoint}" \
    f"_Source={Source}" \
    f"_Output={Output}" \
    f"_UpdateDate={UpdateDate}" \
    f"_TimeCoverage={TimeCoverage}.csv"

    # Download listing if not in blob
    listing = os.path.join(blob_path_,listing_filename)
    if not os.path.isfile(listing):
        response = requests.get(url)
        with open(os.path.join(blob_path_,listing_filename), 'wb') as f:
            f.write(response.content)

    # check if listing is not empty and read url files to be downloaded
    if os.stat(listing).st_size != 0:
        listing_pd = pd.read_csv(listing,header=None)
        listing_pd.rename({0: 'urls'}, axis=1, inplace=True)

        # filenames for the downloaded files
        # urls are either:
        # - "https://ereporting.blob.core.windows.net/downloadservice/    # before 2019
        # - "https://ereporting.blob.core.windows.net/downloadservice/[CountryCode]/ # after 2019
        url_root_download_service_country = f"{url_root_download_service}" \
        f"{CountryCode}/"
        regex_repl = r'^'+url_root_download_service_country
        filenames = listing_pd.replace(to_replace=regex_repl, value='', regex=True)
        regex_repl = r'^'+url_root_download_service
        filenames= filenames.replace(to_replace=regex_repl, value='', regex=True)

        # check which files are already present in the blob
        filenames_path_pd = pd.DataFrame(blob_path_+ os.sep + filenames["urls"])
        file_in_blob = filenames_path_pd.apply(lambda x: os.path.isfile(x['urls']),axis=1)
        filenames_path_pd_no_blob = filenames_path_pd[~file_in_blob]
        listing_pd_no_blob = listing_pd[~file_in_blob]

        # download
        urls = listing_pd_no_blob.urls.tolist()
        filenames_path = filenames_path_pd_no_blob.urls.tolist()
        inputs = zip(urls, filenames_path)
        download_parallel(inputs)

        # update id filename : id sampling points w/ pollutant code
        print('Joining id filenames w/ id sampling points')
        spo_pol_id_file = os.path.join(blob_path_,'spo_pol_id_file.csv')
        if not os.path.isfile(spo_pol_id_file):
            spo_pol_id_file_pd = pd.DataFrame()
        else:
            spo_pol_id_file_pd = pd.read_csv(spo_pol_id_file)

        filenames_ = filenames[~file_in_blob].urls.tolist()
        for i in filenames_:
            filename_path = os.path.join(blob_path_,i)
            tmp = pd.read_csv(filename_path)
            df_pd = pd.DataFrame(data = {'spo_pol_id': [tmp.loc[0,'SamplingPoint']],'filenames': [i]})
            spo_pol_id_file_pd = pd.concat([spo_pol_id_file_pd,df_pd],axis=0,ignore_index=True)

        spo_pol_id_file_pd.to_csv(spo_pol_id_file,index=False)
        print('Download completed.')
    else:
        print('Empty.')

    return True

def download_latest(country:str,pollutant:str,blob_path) -> bool:
    '''
    The function `download_latest` helps retrieving the last 48 hours of non-validated data from reference monitoring stations registered at EEA.

    Args:
        country:
        Country of the monitoring stations.

        pollutant:
        Pollutant of interest.

        blob_path:
        Path of the blob where to store the dataset.
    '''

    # constant urls
    url_root ='https://discomap.eea.europa.eu/map/fme/latest/'

    # dict to dataframe
    country_pd = pd.DataFrame(country_dict)
    pollutant_pd = pd.DataFrame(pollutant_dict)

    # Argument country for the url
    tmp = country_pd.loc[country_pd['country']==country,'shortct']
    if len(tmp)>0:
        CountryCode = tmp.values[0]
    else:
        raise ValueError("The argument 'country' does not refere to any country.")

    # Argument pollutant for the url
    tmp = pollutant_pd.loc[pollutant_pd['pollutant']==pollutant,'shortpl']
    if len(tmp)>0:
        Pollutant = tmp.values[0]
    else:
        raise ValueError("The argument 'pollutant' does not refere to any pollutant")

    # Blob path
    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path.")
    else:
        blob_path_ = os.path.join(blob_path,"latest")
        os.makedirs(blob_path_, exist_ok=True)

    # expected file
    expected_file = f"{CountryCode}_{pollutant}.csv"

    # get listing
    url_listing = f"{url_root}summary.js"
    response = requests.get(url_listing)
    with open(os.path.join(blob_path_,'summary.json'), 'wb') as f:
        f.write(response.content)
    listing = os.path.join(blob_path_,'summary.json')

    if os.stat(listing).st_size != 0:

        listing_pd = pd.read_json(listing)
        existing_files = listing_pd.copy()
        existing_files['filename'] = listing_pd['ct']+'_'+listing_pd['pl']+'.csv'
        matching_files = existing_files.loc[existing_files['filename'].isin([expected_file]),'filename']

        if matching_files.shape[0]>0:

            existing_filenames =  matching_files.values[0]
            url = f"{url_root}{existing_filenames}"
            filename = os.path.join(blob_path_,existing_filenames)

            # download
            inputs = zip([url], [filename])
            download_parallel(inputs)
            print('Download completed.')

        else:
            print('Empty.')
    else:
        print('Empty.')

    return True

def download_metadata(blob_path) -> bool:
    '''
    The function `download_metadata` helps retrieving the metadata of the reference monitoring stations registered at EEA.

    Args:
        blob_path:
        Path of the blob where to store the dataset.
    '''

    # constant url
    url = 'https://discomap.eea.europa.eu/map/fme/metadata/PanEuropean_metadata.csv'

    # Blob path
    if not os.path.exists(blob_path):
        raise ValueError("The argument 'blob_path' targets a non-exising path.")
    else:
        blob_path_ = os.path.join(blob_path,"metadata")
        os.makedirs(blob_path_, exist_ok=True)

    filename = os.path.join(blob_path_,'metadata.csv')

    inputs = zip([url], [filename])
    download_parallel(inputs)
    print('Download completed.')

    return True

def download_parallel(args):
    cpus = cpu_count()
    results = ThreadPool(cpus - 1).imap_unordered(download_url, args)
    for result in results:
        print('url:', result[0], 'time (s):', result[1])

def download_url(args):
    t0 = time.time()
    url, fn = args[0], args[1]
    try:
        r = requests.get(url)
        with open(fn, 'wb') as f:
            f.write(r.content)
        return(url, time.time() - t0)
    except Exception as e:
        print('Exception in download_url():', e)
