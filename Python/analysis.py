import pandas as pd
import h2o
from h2o import H2OFrame
from h2o.automl import H2OAutoML
from data_extractor import initialize_database

engines = initialize_database()
pg_engine = engines[0]
sqlalchemy_engine = engines[1]

df = pd.read_sql_table()