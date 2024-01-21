import sys

import pandas as pd



print(sys.argv)

day = sys.argv[1]

print(f'job finished successfully for day = f{day}')


parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')


#user
#password
#host
#port
#databasename
#table name
#url of the csv


parser.add_argument('user',help ='user name for postgres')
parser.add_argument('password',help ='password for postgres')
parser.add_argument('host',help ='host for postgres')
parser.add_argument('port',help ='port for postgres')
parser.add_argument('databasename',help ='databasename for postgres')
parser.add_argument('table name',help ='table name for postgres')
parser.add_argument('url of the csv',help ='url of the csv')

args = parser.parse_args()
print(args.accumulate(args.integers))