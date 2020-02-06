rm(list = ls())

libs <- c('RPostgreSQL','readr', 'lubridate', 'stringr')

lapply(X = libs, FUN = require, character.only = T)

#Environment variables
PATH <- "/home/ncod/raw_data/"
WRITE_SCHEMA <- 'raw_data'

#readfilenames
file_list <- list.files(path = PATH, pattern = '.csv')
file_paths <- paste(PATH, file_list, sep = '')

#Read data
input_data <- do.call('rbind',lapply(X = file_paths,FUN = function (x){read_csv(file = x, progress = T)}))

#Values like '#' create problems in column names in some languages
names(input_data) <- gsub(pattern = '#',replacement = 'NO',x = names(input_data))

#List out column types
int_cols <- c("BID_LINE_ID", "BID_LINE_DEST_ID", "CUSTOMER_ID", "DESTINATION_ID",
              "FISCAL","BID_NO", "BID_LINE_NO", 
              "MULTI_YEAR_ENDING_FISCAL",
              "DEPOT_NO","PRODUCER_ID","PRODUCT_ID")

int_cols <- c("BID_LINE_ID", "BID_LINE_DEST_ID", "CUSTOMER_ID", "DESTINATION_ID",
              "FISCAL","BID_NO", "BID_LINE_NO", 
              "DEPOT_NO","PRODUCER_ID","PRODUCT_ID")

date_cols <- c("BID_DUE_DATE","EFFECTIVE_DATE", "EXPIRY_DATE", "OPENING_DATE")

str_cols <- c("CURRENCY_UOM", "DISTANCE_UOM", "WEIGHT_UOM","PRICE_COMPETITOR_NAME",
              "SALES_REGION_NAME", "BID_NAME","BID_TYPE_NAME","DESTINATION_NO",
              "CUSTOMER_NAME", "CUSTOMER_COUNTY_NAME", "CUSTOMER_STATE_PROV", 
              "CUSTOMER_TYPE_NAME", "DESTINATION_NAME", "DESTINATION_COUNTY_NAME", 
              "DESTINATION_STATE_PROV", "REQUEST_METHOD","DEPOT_NAME","PRODUCER_NAME",
              "PRODUCT_NAME","AWARD_COMPETITOR_NAME","CUSTOMER_NO","AWARD_COMPETITOR_ID",
              "PRICE_COMPETITOR_ID","DESTINATION_ZIP_POSTAL")
#Note
#Destination ZIP is not an integer because there are values like 66061-7888 in the data

num_cols <- c("TOTAL_QTY_TY", "TOTAL_EF_QTY_TY","MIN_PURCHASE_QTY_TY", "MAX_SUPPLY_QTY_TY",
              "DISTANCE_TY", "STOCKPILE_DEPOT_COST_TY", "DIRECT_TRANSFER_DEPOT_COST_TY", 
              "FREIGHT_COST_TY", "FUEL_SURCHARGE_COST_TY", "EQUIPMENT_COST_TY", 
              "COMMITTED_COST_TY", "COMMITTED_PRICE_TY", "COMMITTED_MR_TY", 
              "COMMITTED_MARGIN_TY","AWARD_PRICE_TY", "AWARD_MR_TY", "AWARD_MARGIN_TY", "SHIPMENT_QTY_TY")

logi_cols <- c("MULTI_YEAR","ROLLOVER","CMP_COSTS_YN")

#Convert dates
input_data[,date_cols] <- lapply(input_data[,date_cols],dmy_hms)
input_data[,date_cols] <- lapply(input_data[,date_cols],date)

#Convert integers
input_data[,int_cols] <- lapply(input_data[,int_cols],as.integer)
#NAs are introduced because some columns are blank for competitors

#Convert string
input_data[,str_cols] <- lapply(input_data[,str_cols],as.character)

#special character in data needs to be replace with appropiate letter
input_data[,str_cols] <- lapply(input_data[,str_cols],function(string){gsub(pattern = '\xe9',
                                                                            replacement = 'e',
                                                                            x = string)})
input_data[,str_cols] <- lapply(input_data[,str_cols],function(string){gsub(pattern = '\xe8',
                                                                            replacement = 'e',
                                                                            x = string)})
input_data[,str_cols] <- lapply(input_data[,str_cols],function(x){str_replace(pattern = '\U3e34663',
                                                                            replacement = 'e',
                                                                            string = x)})

#Convert numeric
input_data[,num_cols] <- lapply(input_data[,num_cols],as.numeric)

#Convert logical
conv_func <- function(x){x <- ifelse(test = x=='Y', yes = 'TRUE', no = 'FALSE'); return(as.logical(x))}
input_data[,logi_cols] <- lapply(input_data[,logi_cols],conv_func)

#Converting to dataframe for faster writing
input_data <- data.frame(input_data)
names(input_data) <- tolower(names(input_data))

#Write to database
con <- dbConnect(PostgreSQL(), host="localhost", user= "admin",password="password", dbname="bid_pricing")

dbWriteTable(conn = con, 
             value = input_data, 
             name = c(WRITE_SCHEMA,gsub(pattern = '-',replacement = '_',x = paste('all_bid_table_',today(),sep=''))), 
             row.names = F, overwrite = T)

dbDisconnect(con)

rm(list = ls())

##Helper if reqd
#Check NAs
#new_DF <- input_data[,int_cols][rowSums(is.na(input_data[,int_cols])) > 0,]

