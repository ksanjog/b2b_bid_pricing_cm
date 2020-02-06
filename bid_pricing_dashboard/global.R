rm(list = ls())

#Read in libraries necessary for the app
print("Started Initializing Apps")

libs <-
  c(
    'shiny',
    'DT',
    'rhandsontable',
    'readr',
    'readxl',
    'shinythemes',
    'shinydashboard',
    'highcharter',
    'magrittr',
    'shinyjs',
    'logging',
    'dplyr',
    'nanotime',
    'shinyBS',
    'assertthat',
    'reshape2'
  )

lapply(X = libs,
       FUN = require,
       character.only = TRUE)

#---- Data Import
dir = "/home/ncod/dashboard_input_data/rds_files/"

prec_ip = readRDS(file = paste0(dir, "prec_ip.rds"))
VI <- readRDS(file = paste0(dir, "VI.rds"))
tree_pred = readRDS(file = paste0(dir, "tree_pred.rds"))
clust_ip = readRDS(file = paste0(dir, "clust_ip.rds"))
imp_keys = readRDS(file = paste0(dir, "imp_keys.rds"))
key_details = readRDS(file = paste0(dir, "key_details.rds"))

# Merge cluster data with key-details data
x = do.call(rbind.data.frame ,strsplit(clust_ip$key,"_"))
names(x) = c("fiscal","destination","bidlineno")
clust_ip = cbind(clust_ip,x)
clust_ip = clust_ip[,c('destination','bidlineno','Cluster')]

key_details$bid_line_no = as.numeric(as.character(key_details$bid_line_no))
key_details$destination_no = as.vector(as.character(key_details$destination_no))
clust_ip$bidlineno = as.numeric(as.character(clust_ip$bidlineno))
clust_ip$destination = as.vector(as.character(clust_ip$destination))


key_details <- key_details %>% 
  left_join(clust_ip, by = c("destination_no" = "destination",
                             "bid_line_no" = "bidlineno"))

#---- Find list of bids/key for dropdown
bid_list = intersect(prec_ip$key, key_details$key)
bid_list = intersect(bid_list, tree_pred$key)
bid_list = intersect(bid_list,imp_keys$key)

select_bids = c(#"2019_H758544_425",
             #"2019_H626269_630",
             "2019_H760366_117",
             "2019_H728685_1413",
             "2019_H761519_42500",
             #"2019_H758544_425",
             "2019_H814630_1430",
             "2019_H602900_279",
             "2019_H751414_1292",
             "2019_H760568_150",
             "2019_H759737_138",
             #"2019_H623960_314",
             "2019_H760365_64",
             "2019_H970438_565",
             #"2019_H813484_25",
             "2019_H760574_126",
             "2019_H760422_20",
             "2019_H761639_31350")
bid_list = bid_list[bid_list %in% select_bids]

bl = bid_list %>% strsplit("_")
fiscal = as.numeric(as.character(lapply(bl, function(l) l[[1]])))
dest = as.vector(as.character(lapply(bl, function(l) l[[2]])))
bidline = as.numeric(as.character(lapply(bl, function(l) l[[3]])))
bl = data.frame(fiscal = (fiscal),
                dest = dest,
                bidline = (bidline))
bl$fiscal = as.numeric(as.character(bl$fiscal))
bl$dest = as.vector(as.character(bl$dest))
bl$bidline = as.numeric(as.character(bl$bidline))


#---- Filter prediction and tree pred with only those keys that are present in 2019 clustering results
# prec_ip = prec_ip[prec_ip$key %in% bid_list, ]
# clust_ip = clust_ip[clust_ip$key %in% bid_list, ]
# tree_pred = tree_pred[tree_pred$key %in% bid_list, ]


#---- Variable Importance Table
imp_df <-
  data.frame('variable' = VI$Feature,
             'importance' = VI$`Relative Importance`, stringsAsFactors = F)
imp_df$importance = as.numeric(gsub("%","",imp_df$importance))

imp_df <- imp_df[1:5,]


#---- Tree predictions for win probability chart
x = do.call(rbind.data.frame ,strsplit(tree_pred$key,"_"))
names(x) = c("fiscal","destination","bidlineno")
tree_pred = cbind(tree_pred,x)



#---- Price Recommendation Table
select_cols = c(
  "key",
  "Predicted_price",
  "error_down_95",
  "error_up_95",
  "estimated_margin",
  "margin_lower",
  "margin_upper"
)
temp = prec_ip[, select_cols]
temp = melt(temp, id = "key")
len_key = length(unique(temp$key))
temp$Category = rep(c(
  rep("Proposed (Average)", len_key),
  rep("Conservative (2.5th Percentile)", len_key),
  rep("Ambitious (97.5th Percentile)", len_key)
), 2)


temp$Metric = c(rep("Price", 3 * len_key), rep("Margin", 3 * len_key))
price_recommendation_df = dcast(temp, key + Category ~ Metric, value.var = "value")
price_recommendation_df$Margin = round(price_recommendation_df$Margin,2)
price_recommendation_df$Price = round(price_recommendation_df$Price,2)

x = do.call(rbind.data.frame ,strsplit(price_recommendation_df$key,"_"))
names(x) = c("fiscal","destination","bidlineno")
price_recommendation_df = cbind(price_recommendation_df,x)



#Clustering table changes
clust_reftable = key_details
names(clust_reftable)
select_clust_cols <- c("key","fiscal","destination_no","bid_line_no",#"customer_no",
                       "customer_name","destination_county_name","destination_state_prov",
                       "bid_due_date","total_qty_ty",#"award_price_ty","award_competitor_id",
                       "cmp_price","cmp_won_bid","top_competitor_id","top_competitor_price",#"last_yr_award_competitor_id",
                       "last_yr_award_price","delta_awardprice_lastyr","incumbent","city_state","Cluster")
names(clust_reftable)

clust_reftable <- clust_reftable[,select_clust_cols]

names(clust_reftable) <- c("key","fiscal","destination_no","bid_line_no",
                           "Customer Name","City","State",
                           "Date","Volume (K Tons)",
                           "CMP Bid Price","CMP Won Bid","Top Competitor","Top Competitor Bid Price",
                           "Bid Price Last Year","Delta YoY AwardPrice","Incumbent","City & State","Cluster")
  

cols_seq_clust_tbl <-
  c('key',
    'fiscal',
    'destination_no',
    'bid_line_no',
    'Cluster',
    'Customer Name',
    'City & State',
    'City',
    'State',
    'Volume (K Tons)',
    'Date',
    'CMP Won Bid',
    'CMP Bid Price',
    'Top Competitor',
    'Top Competitor Bid Price',
    'Bid Price Last Year',
    'Delta YoY AwardPrice',
    'Incumbent')

clust_reftable <- clust_reftable[,cols_seq_clust_tbl]


#Overall performance table

opt_1 <- price_recommendation_df[price_recommendation_df$Category != 'Proposed (Average)',]
opt_1$Category <- ifelse(test = opt_1$Category == 'Ambitious (97.5th Percentile)', yes = 'Net Price vs. Market', no = 'Gap to next best') 
names(opt_1) <- c('key','Price','This Year','Last Year','fiscal','destination','bidlineno')

opt_2 <- opt_1

print("Finished Initializing App")
