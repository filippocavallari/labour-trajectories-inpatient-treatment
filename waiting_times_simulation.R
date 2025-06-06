### 
setwd("/home/cdsw/")

#--------------
# Load packages
#--------------

library(dplyr)
library(sparklyr)
library(lubridate)
library(ggplot2)
library(openxlsx)
library(readxl)
library(stringr)
library(writexl)
library(purrr)

#----------------------------
# Set up the spark connection
#----------------------------

set.seed(123)

spark_config <- spark_config()
spark_config$spark.executor.memory <- "58g"
#spark_config$spark.yarn.executor.memoryOverhead <- "6g"
spark_config$spark.executor.cores <- 5
spark_config$spark.dynamicAllocation.enabled <- "true"
#spark_config$spark.dynamicAllocation.maxExecutors <- 12
spark_config$spark.sql.shuffle.partitions <- 2000
#spark_config$spark.shuffle.service.enabled <- "true"
spark_config$spark.sql.parquet.int96RebaseModeInRead <- "LEGACY" #"CORRECTED"
spark_config$spark.sql.parquet.int96RebaseModeInWrite <- "LEGACY" #"CORRECTED"
spark_config$spark.sql.legacy.timeParserPolicy <- "LEGACY"
spark_config$spark.sql.session.timeZone <- "UTC+01:00" # "GMT"
spark_config$spark.network.timeout <- "600s"
spark_config$spark.driver.maxResultSize <- '10g'

sc <- spark_connect(master = "yarn-client",
                    app_name = "hmrc_outcome_TD",
                    config = spark_config)


#-------
# Waiting times
#-------

## Read in prediction data

age_std_preds <- read_excel("Prediction output/Prediction plots/prediction_data.xlsx") 

age_std <- sparklyr::sdf_copy_to(sc, age_std_preds, overwrite = TRUE) 
 
tbl_change_db(sc, "hmrc_outcome")

data <- spark_read_table(sc, name = "tretspef_hes_data_2404") %>%
filter(elecdate >= "2016-04-01" & elecdate < "2017-04-01")


run_waiting_times <- function(data, filter_list, waiting_time_target, filename) {
  
  diff_pay_list <- list()
  diff_emp_list <- list()
  desc_stats <- list()
  
for (group in filter_list) {

filtered_data <- data %>% filter(tretspef == group)

data_spark <- filtered_data %>%
mutate(months_waited = round(months_between(admidate, elecdate))) %>% 
mutate(months_waited_target = ifelse(months_waited > waiting_time_target, waiting_time_target, months_waited)) %>%
mutate(month_start = as.Date(month_start)) %>%
mutate(admidate_ym = substr(admidate, 1, 7)) %>% 
mutate(admidate_floor = as.Date(paste0(admidate_ym, "-01"))) %>%
mutate(t_op = floor(months_between(month_start, admidate_floor))) %>% 
filter(t_op %in% 0:60) %>%
mutate(months_to_keep = 60 - months_waited) %>%
mutate(months_to_keep_target = 60 - months_waited_target) 

age_std_filt <- age_std %>% filter(tretspef == group)

actual_pay <- data_spark %>%
filter(t_op <= months_to_keep) %>%
left_join(age_std_filt, by = "t_op") %>%
  summarise(actual_pay = sum(effect_pay)) %>%
  collect()

target_pay <- data_spark %>%
filter(t_op <= months_to_keep_target) %>%
left_join(age_std_filt, by = "t_op") %>%
  summarise(target_pay = sum(effect_pay)) %>%
  collect()
  
actual_emp <- data_spark %>%
filter(t_op <= months_to_keep) %>%
left_join(age_std_filt, by = "t_op") %>%
  group_by(census2011_person_id) %>%
  filter(t_op == max(t_op)) %>%
  ungroup() %>%
  summarise(actual_emp = sum(effect_employment)) %>%
  collect()
  
target_emp <- data_spark %>%
filter(t_op <= months_to_keep_target) %>%
left_join(age_std_filt, by = "t_op") %>%
group_by(census2011_person_id) %>%
  filter(t_op == max(t_op)) %>%
  ungroup() %>%
  summarise(target_emp = sum(effect_employment)) %>%
  collect()

  diff_pay <- cbind(actual_pay, target_pay) %>%
  mutate(pay_diff = target_pay - actual_pay,
         tretspef = group,
         wt_target = waiting_time_target) %>%
  collect()

  diff_pay_list[[group]] <- diff_pay
  

  diff_emp <- cbind(actual_emp, target_emp) %>%
  mutate(emp_diff = target_emp - actual_emp,
         tretspef = group,
         wt_target = waiting_time_target) %>%
  collect()
  
  diff_emp_list[[group]] <- diff_emp
  
  
  # desc stats - broken 
  # estimate, median mea, Q1 Q3 min max 
  desc <- data_spark %>%
  filter(t_op == 0) %>%
  mutate(months_waited = as.numeric(months_waited)) %>%
  summarise(total = n(),
            wt_mean = mean(elecdur),
            wt_median = percentile_approx(elecdur, 0.50),
            wt_q1 = percentile_approx(elecdur, 0.25),
            wt_q3 = percentile_approx(elecdur, 0.75),
            wt_min = min(elecdur),
            wt_max = max(elecdur)) %>%
  collect()
  
  wt_perc <- data_spark %>%
  mutate(wt_target = case_when(months_waited <= waiting_time_target ~ "Under target",
                               months_waited > waiting_time_target ~ "Over target")) %>%
  group_by(wt_target) %>% summarise(target_count = n()) %>%
  mutate(perc_over_target = (target_count/sum(target_count)) * 100) %>%
  filter(wt_target == "Over target") %>%
  select(perc_over_target) %>%
  collect()

  comb_desc <- cbind(desc, wt_perc)
  
  comb_desc <- comb_desc %>% as.data.frame %>% mutate(tretspef = group) %>% mutate(wt_target = waiting_time_target)
  
  desc_stats[[group]] <- comb_desc

  
}
  
 pay_diff_df <- bind_rows(diff_pay_list)
 emp_diff_df <- bind_rows(diff_emp_list)
 desc_df <- bind_rows(desc_stats)
  

  wb <- createWorkbook()
  
  # Add worksheets 
  addWorksheet(wb, "Pay")
  addWorksheet(wb, "Employment")
  addWorksheet(wb, "Desc Stats")
  
  
  # Write data to worksheets
  writeData(wb, "Pay", pay_diff_df)
  writeData(wb, "Employment", emp_diff_df)
  writeData(wb, "Desc Stats", desc_df)
  
  # Save the workbook to an Excel file
  saveWorkbook(wb, filename, overwrite = TRUE)
  
}


filter_list <- c("320", "170", "330", "120", 
                 "430", "301", "300", "100", 
                 "502", "400", "150", "130", 
                 "140", "160", "340", "410",
                 "110", "101", "710")

filter_list <- sort(filter_list)


waiting_time_target <- c("1")

run_waiting_times(data, filter_list, waiting_time_target, filename = "benefit_all_years_target_1.xlsx") 

waiting_time_target <- c("2")

run_waiting_times(data, filter_list, waiting_time_target, filename = "benefit_all_years_target_2.xlsx") 

waiting_time_target <- c("3")

run_waiting_times(data, filter_list, waiting_time_target, filename = "benefit_all_years_target_3.xlsx") 

waiting_time_target <- c("4")

run_waiting_times(data, filter_list, waiting_time_target, filename = "benefit_all_years_target_4.xlsx") 
