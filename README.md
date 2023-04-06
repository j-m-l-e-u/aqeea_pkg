# aqeea


[![DOI](https://zenodo.org/badge/621791727.svg)](https://zenodo.org/badge/latestdoi/621791727)

Access to air quality data from the European Environment Agency:
- Data validated by experts from 2013 up to 2 years before the ongoing year.
- up-to-date data without any validation.
- the last 48h.

You can get the main pollutants (i.e NO, NO2, PM2.5, PM10, SO2, O3, CO,C6H6).

The packages aims at:

i) retrieving the datasets from EEA download sources.

ii) filtering the data of your choice once downloaded.

## install from Pypi
```bash
pip install aqeea
```

## install from source
```bash
mkdir tmp
cd /tmp
git clone https://github.com/j-m-l-e-u/aqeea_pkg.git
cd aqeea_pkg
pip3 install .
```

## Usage

The package `aqeea` helps you first to download the datasets, and then to read and to filter them.

Downloading functions can be run both on python and bash.

Reading functions are only available in python.

For help:
```bash
python3 aqeea -h

usage: aqeea [-h] {l_pol,l_city,d_meta,d_hist,d_last} ...

Access to air quality data from the European Environment Agency.

positional arguments:
  {l_pol,l_city,d_meta,d_hist,d_last}
                        sub-command help
    l_pol               Get the list of pollutants measured in the country
    l_city              Get the list of the cities with measurements in the country
    d_meta              Download the metadata of the reference stations
    d_hist              Download historical data up to 2013
    d_last              Download the last 48h non-validated data

optional arguments:
  -h, --help            show this help message and exit
```


## Download the data of your choice:

Downloading can be done either in bash command or in python command.

All the files are saved in a folder (called blob).
Bash command requires the path of the blob to be given.
Python command gets a default path if this one is missing.

You need to download the metadata and the files from the sources of interest before reading the files.


### > The metadata
```bash
#bash
python3 aqeea d_meta -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_metadata
d_metadata()
```

### > Validated PM10 data for year 2021 in France
```bash
#bash
python3 aqeea d_hist -s validated -c France -p PM10 -yf 2021 -yt 2021 -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_historical
d_historical(source='validated',country='France',pollutant='PM10',year_from=2021,year_to=2021)
```
### > Validated data of <i>**all**</i> the pollutants for the <i>**whole**</i> period in Albania

```bash
#bash
python3 aqeea d_hist -s validated -c Albania --pollutants --years -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_historical
d_historical(source='validated',country='Albania',POLLUTANTS=True,YEARS=True)
```

### > Up-to-date PM10 data for year 2023 in Estonia
```bash
#bash
python3 aqeea d_hist -s uptodate -c Estonia -p PM10 -yf 2023 -yt 2023 -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_historical
d_historical(source='uptodate',country='Estonia',pollutant='PM10',year_from=2023,year_to=2023)
```

### > Up-to-date data of <i>**all**</i> the pollutants for the <i>**whole**</i> period in Slovenia

```bash
#bash
python3 aqeea d_hist -s uptodate -c Slovenia --pollutants --years -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_historical
d_historical(source='uptodate',country='Slovenia',POLLUTANTS=True,YEARS=True)
```

### > Latest 48h of PM10 in Serbia
```bash
#bash
python3 aqeea d_last -c Serbia -p PM10 -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_latest
d_latest('Serbia','PM10')
```

### > Latest 48h of PM10 in <i>**all**</i> countries registered at EEA
```bash
#bash
python3 aqeea d_last -p PM10 --countries -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_latest
d_latest('PM10',COUNTRIES=True)
```

### > Latest 48h dataset of <i>**all**</i> the pollutants in Poland
```bash
#bash
python3 aqeea d_last -c Poland --pollutants -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_latest
d_latest('Poland',POLLUTANTS=True)
```

### > Latest 48h dataset of <i>**all**</i> the pollutants in <i>**all**</i> countries registered at EEA
```bash
#bash
python3 aqeea d_last --countries --pollutants -b "/path/to/existing/folder"
```
```python
#python
from aqeea.download import d_latest
d_latest(COUNTRIES=True, POLLUTANTS=True)
```

## Read and filter the downloaded datasets:

The chosen sampling points providing the measurements are filtered by their measurement date and the lat/lon's of the boundary box of your choice.
Open-street Map through the package `osmnx` helps you getting this information.

To install `osmnx`, just type:
```bash
#bash
pip install osmnx
```

### > Get the validated PM10 data in Paris, France from 2021-01-01 00:00:00 UTC to 2021-01-01 12:00:00 UTC
```python
#python
import osmnx as ox
from aqeea.read import historical

# define the place query
query = {'city': 'Paris'}

# get the boundaries of the place
gdf = ox.geocode_to_gdf(query)
bbox = gdf.total_bounds

lat_min, lat_max = bbox[1],bbox[3]
lon_min, lon_max = bbox[0],bbox[2]

res = historical(source='validated',pollutant='PM10',datetime_from='2021-01-01 00:00:00',datetime_to='2021-01-01 12:00:00',lat_min=lat_min, lat_max=lat_max,lon_min=lon_min, lon_max=lon_max)
```

### > Get the up-to-date PM10 data in Tallinn, Estonia from 2023-01-01 00:00:00 UTC to 2023-01-12 00:00:00 UTC
```python
#python
import osmnx as ox
from aqeea.read import historical

# define the place query
query = {'city': 'Tallinn'}

# get the boundaries of the place
gdf = ox.geocode_to_gdf(query)
bbox = gdf.total_bounds

lat_min, lat_max = bbox[1],bbox[3]
lon_min, lon_max = bbox[0],bbox[2]

res = historical(source='uptodate',pollutant='PM10',datetime_from='2023-01-01 00:00:00',datetime_to='2023-01-12 00:00:00',lat_min=lat_min, lat_max=lat_max,lon_min=lon_min, lon_max=lon_max)
```

### > Get the last 48h PM10 data in Paris,France
```python
#python
from aqeea.read import main_read_latest
res = latest(pollutant='PM10',lat_min=48.8155755, lat_max=48.902156,lon_min=2.224122, lon_max=2.4697602)
```

### > Get the metadata of PM10 concentration in Paris,France
```python
#python
from aqeea.read import main_read_metadata
res = metadata(pollutant='PM10',lat_min=48.8155755, lat_max=48.902156,lon_min=2.224122, lon_max=2.4697602)
```

## Notebook examples
See folder `examples`
