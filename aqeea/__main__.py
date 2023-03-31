
import sys
import argparse
from pathlib import Path

from aqeea.listing import pollutant_by_country, city_by_country
from aqeea.download import d_metadata, d_historical, d_latest

def parse():

    # create the top-level parser
    parser = argparse.ArgumentParser(prog='aqeea',description='Access to air quality data from the European Environment Agency.')
    subparsers = parser.add_subparsers(dest='action', help='sub-command help')

    # parser list of pollutant
    parser_list_pollutant = subparsers.add_parser('l_pol',help='Get the list of pollutants measured in the country')
    parser_list_pollutant.add_argument("-c", type=str, help="Country", required=True)

    # parser list of pollutant
    parser_list_city = subparsers.add_parser('l_city',help='Get the list of the cities with measurements in the country')
    parser_list_city.add_argument("-c", type=str, help="Country", required=True)

    # parser download metadata
    parser_dwld_metadata = subparsers.add_parser('d_meta',help='Download the metadata of the reference stations')
    parser_dwld_metadata.add_argument("-b", type=str, help="Path to the blob")

    # parser download historical
    parser_dwld_historical = subparsers.add_parser('d_hist',help='Download historical data up to 2013')
    parser_dwld_historical.add_argument("-s", type=str, help="Source of interest: validated or uptodate",required=True)
    parser_dwld_historical.add_argument("--countries", action='store_true', help="All the countries")
    parser_dwld_historical.add_argument("-c", type=str, help="Country")
    parser_dwld_historical.add_argument("--pollutants", action='store_true', help="All the pollutant")
    parser_dwld_historical.add_argument("-p", type=str, help="Pollutant")
    parser_dwld_historical.add_argument("--years", action='store_true', help="All the years")
    parser_dwld_historical.add_argument("-yf", type=int, help="Starting year")
    parser_dwld_historical.add_argument("-yt", type=int, help="Ending year")
    parser_dwld_historical.add_argument("-b", type=str, help="Path to the blob")

    # parser download latest
    parser_dwld_latest = subparsers.add_parser('d_last',help='Download the last 48h non-validated data')
    parser_dwld_latest.add_argument("--countries", action='store_true', help="All the countries")
    parser_dwld_latest.add_argument("-c", type=str, help="Country")
    parser_dwld_latest.add_argument("--pollutants", action='store_true', help="All the pollutant")
    parser_dwld_latest.add_argument("-p", type=str, help="Pollutant")
    parser_dwld_latest.add_argument("-b", type=str, help="Path to the blob")

    return parser.parse_args()

def main():
    """
    Function to populate the blob with air quality data timeseries from EEA.
    """

    arg = parse()

    if arg.action == 'l_pol':
        res = pollutant_by_country(country=arg.c)
        print(res)
    elif arg.action == 'l_city':
        res = city_by_country(country=arg.c)
        print(res)
    elif arg.action == 'd_meta':
        res = d_metadata(blob_path=arg.b)
    elif arg.action == 'd_hist':
        res = d_historical(source=arg.s,COUNTRIES=arg.countries,POLLUTANTS=arg.pollutants, YEARS = arg.years, country=arg.c, pollutant=arg.p, year_from=arg.yf, year_to=arg.yt,blob_path=arg.b)
    elif arg.action == 'd_last':
        res = d_latest(COUNTRIES=arg.countries,POLLUTANTS=arg.pollutants, country=arg.c, pollutant=arg.p, blob_path=arg.b)
    else:
        sys.exit("{} does not exists. Add an action to be done.".format(arg.action))


if __name__ == "__main__":
    sys.exit(main())
