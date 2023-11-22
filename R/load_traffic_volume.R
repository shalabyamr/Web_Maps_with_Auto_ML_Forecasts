library(opendatatoronto)
library(dplyr)
library(RPostgreSQL)


connec <- dbConnect(RPostgres::Postgres(), 
                    dbname = 'postgres',
                    host = '127.0.0.1', 
                    port = 5433,
                    user = 'postgres', 
                    password = 'postgres')
connec
package <- show_package("traffic-volumes-at-intersections-for-all-modes")
resources <- list_package_resources("traffic-volumes-at-intersections-for-all-modes")
datastore_resources <- filter(resources, tolower(format) %in% c('csv'))
df <- filter(datastore_resources, row_number()==1) %>% get_resource()
df['last_updated'] <- Sys.time()
write.csv(df, 'stg_traffic_volume.csv', row.names = FALSE)
dbWriteTable(conn = connec, name = 'stg_traffic_volume', value = df, append = TRUE, row.names = FALSE)
db_write_table(con = connec, table = 'stg_traffic_volume', )
