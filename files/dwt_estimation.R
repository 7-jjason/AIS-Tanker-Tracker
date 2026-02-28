# Estimate dwt of tankers from sample data using equasis

file_list <- list.files("data/processed", 
                        all.files = TRUE,
                        full.names = TRUE)
file_list <- tail(file_list, 30)
files <- lapply(file_list, readRDS)
data <- lapply(files, function(file_id) {
  return(file_id[[1]])
})
data <- rbindlist(data)
setDT(data)
data <- data[, .(mmsi, imo, length_m, beam_m, draught_change_m, draught_end_m)]
data <- data[imo != 0, ] 
data <- data[!duplicated(data$imo)]

# Create a vector/append a dwt column to the dataset, manually pulling dwt from 
# equasis (gross tonnage included but not used)
data[, `:=`(dwt = c(
           16628,
           2690,
           6798,
           6758,
           3606,
           7348,
           2973,
           7990,
           313992,
           6956,
           6379,
           7511,
           7286,
           42000,
           40059,
           49996,
           6248,
           109999,
           109999,
           112459,
           50017,
           49929,
           50295,
           109896,
           5518, # 25
           158889,
           157447,
           46184,
           49828,
           49999,
           494539,
           193048,
           4769,
           4743,
           5846,
           50192,
           50359,
           7746,
           10343,
           29543,
           30212,
           123602,
           7999,
           8648,
           7996,
           9435,
           9027,
           6346,
           6551,
           22554, # 50
           18561,
           8501,
           6065,
           4999,
           16587,
           17596,
           16651,
           8084,
           8063,
           12984,
           7439,
           60648,
           59738,
           55001,
           27126,
           94494,
           18818,
           11289,
           9337,
           6631,
           11161,
           5756,
           5470,
           4389,
           4710, # 75
           17998,
           4514,
           3550,
           3480,
           72562,
           15988,
           7051,
           7770,
           6267,
           NA, # 85; imo = 2330121
           114264,
           18467,
           19998 # 88
         ),
         gt = c(
           11948,
           1864,
           3892,
           3898,
           2239,
           4776,
           1930,
           4903,
           166094,
           4969,
           4999,
           4292,
           4081,
           25918,
           25443,
           29513,
           4599,
           66970,
           65145,
           63532,
           29762,
           29447,
           29725,
           62849,
           4062, # 25
           81409,
           81502,
           27376,
           29801,
           29307,
           29993,
           110693,
           4261,
           4241,
           3726,
           30964,
           30965,
           5366,
           9175,
           25952,
           26583,
           85504,
           5997,
           5815,
           5972,
           5153,
           5198,
           4696,
           4690,
           18636, # 50
           13283,
           5770,
           5539,
           3308,
           11943,
           11472,
           11935,
           5573,
           5589,
           8446,
           4758,
           56222,
           60783,
           48229,
           16084,
           115366,
           12008,
           7315,
           5219,
           4391,
           7244,
           3992,
           3648,
           3977,
           2990, # 75
           11711,
           2956,
           2222,
           2522,
           38889,
           10917,
           4816,
           5803,
           4364,
           NA, # 85; imo = 2330121
           62914,
           11944,
           13097 # 88
         ))]

# clean na - 1 observation
data <- data[!is.na(dwt) & !is.na(gt)]
write.csv(data, "data/regression_data.csv")
data <- read.csv("data/regression_data.csv")

# Run statistics
data[, !c("mmsi", "imo")] |>
  cor()

# Run a regression (did not include ship type because estimating an estimation isn't great)
dwt_mod <- lm(log(dwt) ~ log(length_m), data = data) # 2
dwt_sum <- summary(dwt_mod)
resid_se <- dwt_sum$sigma

setDT(data)
data[, ln_dwt_est := -5.7435 + 3.1366 * log(length_m)]
data[, dwt_est := exp(ln_dwt_est)]
data[, test := (dwt_est - dwt) / dwt]
mean(data$test) 
mean(abs(data$test)) 



