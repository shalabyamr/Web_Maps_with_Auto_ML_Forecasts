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
dbWriteTable(conn = connec, name = 'public.stage.stage_traffic_volume', value = df, append = TRUE, row.names = FALSE)
