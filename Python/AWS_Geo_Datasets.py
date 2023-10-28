import pandas as pd

url = 'https://github.com/giswqs/aws-open-data-geo/raw/master/aws_geo_datasets.tsv'
df = pd.read_csv(url, sep='\t')
df.to_csv('/Users/amr/PycharmProjects/ggr473/Data/aws_geo_datasets.csv')