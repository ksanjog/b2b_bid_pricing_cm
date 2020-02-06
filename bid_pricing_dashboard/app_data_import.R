rm(list = ls())

library(readr)

#ENV variables

INPUT_PATH <- '/home/ncod/dashboard_input_data/raw_files/'
OUTPUT_PATH <- '/home/ncod/dashboard_input_data/rds_files/'

PRICE_RECOMM_FILE <- 'price_recomm_20200124.csv'
VI_FILE <- 'varimp_v1.csv'
TREE_PRED_FILE <- 'price_alltreepredictions_0127.csv'
CLUSTER_OUTPUT <- 'Clustering_results_Supervised_20200127.csv'
IMPORTANT_KEYS_FILE <- 'ImportantKeys.csv'
KEYDETAILS_FILE <- 'key_details_referencetable_0129.csv'

prec_ip = read_csv(paste0(INPUT_PATH,PRICE_RECOMM_FILE))
VI <- read_csv(paste0(INPUT_PATH,VI_FILE))
tree_pred = read_csv(paste0(INPUT_PATH,TREE_PRED_FILE))
clust_ip = read_csv(paste0(INPUT_PATH,CLUSTER_OUTPUT))
imp_keys = read_csv(paste0(INPUT_PATH,IMPORTANT_KEYS_FILE))
key_details = read_csv(paste0(INPUT_PATH,KEYDETAILS_FILE))

saveRDS(object = prec_ip,file = paste(OUTPUT_PATH,'prec_ip.rds', sep = ''))
saveRDS(object = VI,file = paste(OUTPUT_PATH,'VI.rds', sep = ''))
saveRDS(object = tree_pred,file = paste(OUTPUT_PATH,'tree_pred.rds', sep = ''))
saveRDS(object = clust_ip,file = paste(OUTPUT_PATH,'clust_ip.rds', sep = ''))
saveRDS(object = imp_keys,file = paste(OUTPUT_PATH,'imp_keys.rds', sep = ''))
saveRDS(object = key_details,file = paste(OUTPUT_PATH,'key_details.rds', sep = ''))

rm(list = ls())
