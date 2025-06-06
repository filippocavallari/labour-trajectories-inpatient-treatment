#--------------
# Load packages
#--------------

library(dplyr)
library(sparklyr)
library(ggplot2)
library(uuid)
library(dbplyr)
#library(writexl)

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


# Read in HES data

sdf_hes_2015 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2015_std")
tretspef_2015 <- sdf_sql(sc, "SELECT epikey, tretspef FROM hospital_episode_statistics.apc_hes_2015_extra_std")

data_2015 <- sdf_hes_2015 %>% mutate(epikey = epikey_hes) %>% left_join(tretspef_2015, by = "epikey") %>% 
                              mutate(tretspef_hes = tretspef) %>% select(-tretspef)

sdf_hes_2016 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2016_std")
tretspef_2016 <- sdf_sql(sc, "SELECT epikey, tretspef FROM hospital_episode_statistics.apc_hes_2016_extra_std")

data_2016 <- sdf_hes_2016 %>% mutate(epikey = epikey_hes) %>% left_join(tretspef_2016, by = "epikey") %>% 
                              mutate(tretspef_hes = tretspef) %>% select(-tretspef)

sdf_hes_2017 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2017_std")
tretspef_2017 <- sdf_sql(sc, "SELECT epikey, tretspef FROM hospital_episode_statistics.apc_hes_2017_extra_std")

data_2017 <- sdf_hes_2017 %>% mutate(epikey = epikey_hes) %>% left_join(tretspef_2017, by = "epikey") %>% 
                              mutate(tretspef_hes = tretspef) %>% select(-tretspef)

sdf_hes_2018 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2018_std")
tretspef_2018 <- sdf_sql(sc, "SELECT epikey, tretspef FROM hospital_episode_statistics.apc_hes_2018_extra_std")

data_2018 <- sdf_hes_2018 %>% mutate(epikey = epikey_hes) %>% left_join(tretspef_2018, by = "epikey") %>% 
                              mutate(tretspef_hes = tretspef) %>% select(-tretspef)

sdf_hes_2019 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2019_std")
tretspef_2019 <- sdf_sql(sc, "SELECT epikey, tretspef FROM hospital_episode_statistics.apc_hes_2019_extra_std")

data_2019 <- sdf_hes_2019 %>% mutate(epikey = epikey_hes) %>% left_join(tretspef_2019, by = "epikey") %>% 
                              mutate(tretspef_hes = tretspef) %>% select(-tretspef)

sdf_hes_2019_2020 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2019_2020_ar_std")
data_2019_2020 <- sdf_hes_2019_2020 %>% mutate(epikey = epikey_hes) %>% left_join(tretspef_2019, by = "epikey") %>% 
                                        mutate(tretspef_hes = tretspef) %>% select(-tretspef)

data_2020 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, tretspef_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2020_std")
data_2021 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, tretspef_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2021_std")
data_2022 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, tretspef_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2022_std")
data_2023 <- sdf_sql(sc, "SELECT admiage_hes, admimeth_hes, admidate_hes, elecdate_hes, elecdur_hes, newnhsno_hes, tretspef_hes, epikey_hes FROM hospital_episode_statistics.apc_hes_2023_std")


sdf_hes_ip <- sdf_bind_rows(data_2015,
                            data_2016,
                            data_2017,
                            data_2018,
                            data_2019,
                            data_2019_2020,
                            data_2020,
                            data_2021,
                            data_2022,
                            data_2023)

sdf_nrow(sdf_hes_ip)

sdf_hes_ip <- sdf_hes_ip %>%
  mutate(admidate = admidate_hes,
         elecdate = elecdate_hes,
         elecdur = elecdur_hes,
         nhsnumber = newnhsno_hes,
         tretspef = tretspef_hes,
         admimeth = admimeth_hes,
         age_at_index_date = admiage_hes,
         epikey = epikey_hes) %>%
  select(admidate, elecdate, elecdur, nhsnumber, tretspef, admimeth, age_at_index_date, epikey)

sdf_hes_ip %>% group_by(epikey) %>% summarise(count=n()) %>% arrange(desc(count)) %>% head()

# Filter to those with an nhs_number
sdf_hes_ip <- filter(sdf_hes_ip, !is.na(nhsnumber))

# Has a treatment code
sdf_hes_ip <- filter(sdf_hes_ip, !is.na(tretspef) & tretspef != "&")

# Admin method = non emergency
sdf_hes_ip <- sdf_hes_ip %>% filter(admimeth %in% 11:13)

# Has a valid elecdate from start of data
sdf_hes_ip <- sdf_hes_ip %>%
  mutate(elecdate = to_date(elecdate, "yyyy-MM-dd")) %>% 
  filter(!is.na(elecdate) & elecdate != "1800-01-01" & elecdate != "1801-01-01") %>%
  filter(elecdate >= "2015-04-01")

# Has an elecdur waiting time
sdf_hes_ip <- sdf_hes_ip %>% filter(!is.na(elecdur))

# Has an elecdur waiting time within 2 years
sdf_hes_ip <- sdf_hes_ip %>% filter(elecdur <= 730)

# Filter to earliest date for each person and treatment code
earliest_records <- sdf_hes_ip %>%
  group_by(nhsnumber, tretspef) %>%
  summarise(earliest_date = min(elecdate, na.rm = TRUE), .groups = "drop") %>%
  ungroup() %>%
  sdf_drop_duplicates() %>%
  mutate(earliest_date_flag = 1)

sdf_hes_ip <- sdf_hes_ip %>%
  left_join(earliest_records, by = c("nhsnumber", "tretspef", "elecdate" = "earliest_date")) %>%
  filter(earliest_date_flag == 1) %>%
  sdf_drop_duplicates() %>%
  select(-earliest_date_flag)

# Remove imperfect dupes based on different diagnosis codes - less than 1% of population
dupes <- sdf_hes_ip %>%
  group_by(nhsnumber, tretspef) %>% 
  count() %>% 
  ungroup()  

sdf_hes_ip <- sdf_hes_ip %>%
  left_join(dupes, by = c("nhsnumber", "tretspef")) %>%
  filter(n == 1) %>%
  select(-n)

# check max one row per person per treatment
sdf_hes_ip %>% group_by(nhsnumber, tretspef) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()

# Filter to be 24:64 on index date
sdf_hes_ip_age_filt <- sdf_hes_ip %>% 
  dplyr::filter(age_at_index_date >= 24, age_at_index_date < 65)


# Create monthly data

months <- data.frame(month_start = seq(as.Date("2015-04-01"), as.Date("2029-03-01"), by = "month"))
months_sdf <- sdf_copy_to(sc, months, overwrite = TRUE)

sdf_monthly <- sdf_hes_ip_age_filt %>% 
  mutate(admidate = as.Date(admidate)) %>%
  cross_join(months_sdf) %>% 
  mutate(month_diff = (year(month_start) - year(admidate)) * 12 + (month(month_start) - month(admidate)), 
         age_month = age_at_index_date + month_diff / 12) %>%
  mutate(month_start = as.Date(month_start)) %>%
  mutate(admidate_ym = substr(admidate, 1, 7)) %>% 
  mutate(admidate_floor = as.Date(paste0(admidate_ym, "-01"))) %>%
  mutate(t_op = floor(months_between(month_start, admidate_floor)))

sdf_monthly %>% filter(t_op == 0) %>% group_by(month_start, admidate) %>% count() %>% ungroup() %>% arrange(month_start, admidate) %>% data.frame() %>% head(n=100)
sdf_monthly %>% filter(t_op == -1) %>% group_by(month_start, admidate) %>% count() %>% ungroup() %>% arrange(month_start, admidate) %>% data.frame() %>% head(n=100)

# Apply monthly age filter

sdf_monthly <- sdf_monthly %>% filter(age_month >= 30 & age_month < 59)

sdf_monthly %>% summarise(min = min(age_month), 
                          max = max(age_month))


spark_write_table(sdf_monthly, name = "cen_dth_ecds.hes_2015_2024_0205", format = "parquet", mode = "overwrite")


### Waiting time simulation ###

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
#library(writexl)
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

age_std_preds <- read_excel("prediction_data.xlsx") 

age_std <- sparklyr::sdf_copy_to(sc, age_std_preds, overwrite = TRUE) 
 
tbl_change_db(sc, "cen_dth_ecds")

data <- spark_read_table(sc, name = "hes_2015_2024_0205") 


run_waiting_times <- function(data, filter_list, waiting_time_target, filename, number_of_months_list) {
  
  diff_pay_list <- list()
  diff_emp_list <- list()
  desc_stats <- list()
  
  for (group in filter_list) {
    
    for (number_of_months in number_of_months_list) {

    filtered_data <- data %>% filter(tretspef == group)

    data_spark <- filtered_data %>%

      mutate(months_waited_raw = months_between(admidate, elecdate)) %>%
      mutate(months_waited = ceiling(months_between(admidate, elecdate))) %>% 
      mutate(months_waited_target = ifelse(months_waited > waiting_time_target, waiting_time_target, months_waited)) %>%

      mutate(month_start = as.Date(month_start)) %>%
      mutate(admidate_ym = substr(admidate, 1, 7)) %>% 
      mutate(admidate_floor = as.Date(paste0(admidate_ym, "-01"))) %>%
      mutate(t_op = floor(months_between(month_start, admidate_floor))) %>% 
      filter(t_op %in% 0:number_of_months) %>%

      mutate(months_to_keep = number_of_months - months_waited) %>%
      mutate(months_to_keep_target = number_of_months - months_waited_target) %>%
      mutate(time_period = case_when(elecdate >= "2015-04-01" & elecdate < "2016-04-01" ~ "2015/16",
                                    elecdate >= "2016-04-01" & elecdate < "2017-04-01" ~ "2016/17",
                                    elecdate >= "2017-04-01" & elecdate < "2018-04-01" ~ "2017/18",
                                    elecdate >= "2018-04-01" & elecdate < "2019-04-01" ~ "2018/19",
                                    elecdate >= "2019-04-01" & elecdate < "2020-04-01" ~ "2019/20",
                                    elecdate >= "2020-04-01" & elecdate < "2021-04-01" ~ "2020/21",
                                    elecdate >= "2021-04-01" & elecdate < "2022-04-01" ~ "2021/22",

                                    elecdate >= "2022-04-01" & elecdate < "2022-05-01" ~ "FYE2023/M01",
                                    elecdate >= "2022-05-01" & elecdate < "2022-06-01" ~ "FYE2023/M02",
                                    elecdate >= "2022-06-01" & elecdate < "2022-07-01" ~ "FYE2023/M03",
                                    elecdate >= "2022-07-01" & elecdate < "2022-08-01" ~ "FYE2023/M04",
                                    elecdate >= "2022-08-01" & elecdate < "2022-09-01" ~ "FYE2023/M05",
                                    elecdate >= "2022-09-01" & elecdate < "2022-10-01" ~ "FYE2023/M06",
                                    elecdate >= "2022-10-01" & elecdate < "2022-11-01" ~ "FYE2023/M07",
                                    elecdate >= "2022-11-01" & elecdate < "2022-12-01" ~ "FYE2023/M08",
                                    elecdate >= "2022-12-01" & elecdate < "2023-01-01" ~ "FYE2023/M09",
                                    elecdate >= "2023-01-01" & elecdate < "2023-02-01" ~ "FYE2023/M10",
                                    elecdate >= "2023-02-01" & elecdate < "2023-03-01" ~ "FYE2023/M11",
                                    elecdate >= "2023-03-01" & elecdate < "2023-04-01" ~ "FYE2023/M12",

                                    elecdate >= "2023-04-01" & elecdate < "2023-05-01" ~ "FYE2024/M01",
                                    elecdate >= "2023-05-01" & elecdate < "2023-06-01" ~ "FYE2024/M02",
                                    elecdate >= "2023-06-01" & elecdate < "2023-07-01" ~ "FYE2024/M03",
                                    elecdate >= "2023-07-01" & elecdate < "2023-08-01" ~ "FYE2024/M04",
                                    elecdate >= "2023-08-01" & elecdate < "2023-09-01" ~ "FYE2024/M05",
                                    elecdate >= "2023-09-01" & elecdate < "2023-10-01" ~ "FYE2024/M06",
                                    elecdate >= "2023-10-01" & elecdate < "2023-11-01" ~ "FYE2024/M07",
                                    elecdate >= "2023-11-01" & elecdate < "2023-12-01" ~ "FYE2024/M08",
                                    elecdate >= "2023-12-01" & elecdate < "2024-01-01" ~ "FYE2024/M09",
                                    elecdate >= "2024-01-01" & elecdate < "2024-02-01" ~ "FYE2024/M10",
                                    elecdate >= "2024-02-01" & elecdate < "2024-03-01" ~ "FYE2024/M11",
                                    elecdate >= "2024-03-01" & elecdate < "2024-04-01" ~ "FYE2024/M12"#,

                                    #elecdate >= "2022-04-01" & elecdate < "2023-04-01" ~ "2022/23",
                                    #elecdate >= "2023-04-01" & elecdate < "2024-04-01" ~ "2023/24"
                                    )) 

    age_std_filt <- age_std %>% filter(tretspef == group)

    actual_pay <- data_spark %>%
      filter(t_op <= months_to_keep) %>%
      left_join(age_std_filt, by = "t_op") %>%
      group_by(time_period) %>%
      summarise(actual_pay_value = sum(effect_pay)) %>%
      collect()

    target_pay <- data_spark %>%
      filter(t_op <= months_to_keep_target) %>%
      left_join(age_std_filt, by = "t_op") %>%
      group_by(time_period) %>%
      summarise(target_pay_value = sum(effect_pay)) %>%
      collect()
      
    actual_emp <- data_spark %>%
      filter(t_op <= months_to_keep) %>%
      left_join(age_std_filt, by = "t_op") %>%
      group_by(time_period) %>%
      summarise(actual_emp_value = sum(effect_employment)) %>%
      collect()

    target_emp <- data_spark %>%
      filter(t_op <= months_to_keep_target) %>%
      left_join(age_std_filt, by = "t_op") %>%
      group_by(time_period) %>%
      summarise(target_emp_value = sum(effect_employment)) %>%
      collect()

     diff_pay <- actual_pay %>%
      left_join(target_pay, by = "time_period") %>%
      mutate(pay_diff = target_pay_value - actual_pay_value,
             tretspef = group,
             wt_target = waiting_time_target,
             months_after_treatment = number_of_months) %>%
      arrange(time_period) %>%
      collect()

    diff_pay_list[[paste0(group,"_", number_of_months)]] <- diff_pay

    diff_emp <- actual_emp %>%
      left_join(target_emp, by = "time_period") %>%
      mutate(emp_diff = target_emp_value - actual_emp_value,
             tretspef = group,
             wt_target = waiting_time_target,
             months_after_treatment = number_of_months) %>%
      arrange(time_period) %>%
      collect()

    diff_emp_list[[paste0(group,"_", number_of_months)]] <- diff_emp


    # desc stats 
    # estimate, median, mean, Q1 Q3 min max 
    desc <- data_spark %>%
      filter(t_op == 0) %>%
      group_by(time_period) %>%
      mutate(months_waited = as.numeric(months_waited)) %>%
      summarise(total = n(),
                wt_mean = mean(elecdur),
                wt_median = percentile_approx(elecdur, 0.50),
                wt_q1 = percentile_approx(elecdur, 0.25),
                wt_q3 = percentile_approx(elecdur, 0.75),
                wt_min = min(elecdur),
                wt_max = max(elecdur)) %>%
      arrange(time_period) %>%
      collect()

    wt_perc <- data_spark %>%
      mutate(wt_target = case_when(months_waited <= waiting_time_target ~ "Within target",
                                   months_waited > waiting_time_target ~ "Over target")) %>%
      group_by(time_period, wt_target) %>% 
      summarise(target_count = n()) %>%
      mutate(perc_over_target = (target_count/sum(target_count)) * 100) %>%
      filter(wt_target == "Over target") %>%
      select(perc_over_target) %>%
      collect()

    comb_desc <- desc %>% left_join(wt_perc, by = "time_period")

    comb_desc <- comb_desc %>% as.data.frame %>% mutate(tretspef = group) %>% mutate(wt_target = waiting_time_target) %>% arrange(time_period)

    desc_stats[[paste0(group,"_" ,number_of_months)]] <- comb_desc


  }
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

number_of_months_list <- c("60")

waiting_time_target <- c("1")

run_waiting_times(data, filter_list, waiting_time_target, filename = "benefit_separate_years_target_1_1405.xlsx", number_of_months_list) 

waiting_time_target <- c("2")

run_waiting_times(data, filter_list, waiting_time_target, filename = "benefit_separate_years_target_2_1405.xlsx", number_of_months_list) 

waiting_time_target <- c("3")

run_waiting_times(data, filter_list, waiting_time_target, filename = "benefit_separate_years_target_3_1405.xlsx",number_of_months_list) 

waiting_time_target <- c("4")

run_waiting_times(data, filter_list, waiting_time_target, filename = "benefit_separate_years_target_4_1405.xlsx",number_of_months_list) 

