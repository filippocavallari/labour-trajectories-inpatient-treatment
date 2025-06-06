setwd("/home/cdsw/")

#--------------
# Load packages
#--------------

library(dplyr)
library(sparklyr)
library(lubridate)
library(ggplot2)
library(tidyverse)
library(openxlsx)
library(stringr)

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

###

# Read in dataset

tbl_change_db(sc, "hmrc_outcome")

data <- spark_read_table(sc, name = "tretspef_hes_data_2404")

data_mutated <- data %>% 
  mutate(month_start = as.Date(month_start)) %>%
  mutate(admidate_ym = substr(admidate, 1, 7)) %>% 
  mutate(admidate_floor = as.Date(paste0(admidate_ym, "-01"))) %>%
  mutate(t_op = floor(months_between(month_start, admidate_floor))) %>%  
  filter(t_op %in% -60:60) %>%
  mutate(qrt_age = floor(age_month / 0.25) * 0.25) %>%
  mutate(flag_variable = case_when(elecdur <= 126 ~ "Within 18 weeks",
                                   elecdur > 126 ~ "More than 18 weeks")) %>%
  mutate(age_at_index_date_grouped = case_when(age_at_index_date < 16 ~ "0 to 15",
                                               age_at_index_date >= 16 & age_at_index_date < 20 ~ "16 to 19",
                                               age_at_index_date >= 20 & age_at_index_date < 25 ~ "20 to 24",
                                               age_at_index_date >= 25 & age_at_index_date < 30 ~ "25 to 29",
                                               age_at_index_date >= 30 & age_at_index_date < 35 ~ "30 to 34",
                                               age_at_index_date >= 35 & age_at_index_date < 40 ~ "35 to 39",
                                               age_at_index_date >= 40 & age_at_index_date < 45 ~ "40 to 44",
                                               age_at_index_date >= 45 & age_at_index_date < 50 ~ "45 to 49",
                                               age_at_index_date >= 50 & age_at_index_date < 55 ~ "50 to 54",
                                               age_at_index_date >= 55 & age_at_index_date < 60 ~ "55 to 59",
                                               age_at_index_date >= 60 & age_at_index_date < 65 ~ "60 to 64",
                                               age_at_index_date >= 65 ~ "65 and over")) %>%

  mutate(age_month_grouped = case_when(age_month >= 30 & age_month < 35 ~ "30 to 34",
                                               age_month >= 35 & age_month < 40 ~ "35 to 39",
                                               age_month >= 40 & age_month < 45 ~ "40 to 44",
                                               age_month >= 45 & age_month < 50 ~ "45 to 49",
                                               age_month >= 50 & age_month < 55 ~ "50 to 54",
                                               age_month >= 55 & age_month < 60 ~ "55 to 58")) 


data_mutated %>% filter(t_op == 0) %>% group_by(month_start, admidate) %>% count() %>% ungroup() %>% arrange(month_start, admidate) %>% data.frame() %>% head(n=100)
data_mutated %>% filter(t_op == -1) %>% group_by(month_start, admidate) %>% count() %>% ungroup() %>% arrange(month_start, admidate) %>% data.frame() %>% head(n=100)


sdf_nrow(data_mutated)
sdf_nrow(distinct(data_mutated, census2011_person_id))

data_mutated %>% group_by(census2011_person_id, tretspef, t_op) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()

# create person month level dataset for calculating quantiles below
# we round pay to 3dp for this calculation due to a duplication issue
data_mutated_person_month_level <- data_mutated %>%
  select(census2011_person_id, year_month, pay) %>%
  mutate(pay = round(pay, 2)) %>%
  sdf_drop_duplicates()

sdf_nrow(data_mutated_person_month_level)
sdf_nrow(distinct(data_mutated_person_month_level, census2011_person_id))

data_mutated_person_month_level %>% group_by(census2011_person_id, year_month) %>% count() %>% ungroup() %>% arrange(desc(n)) %>% head()

# create outcome variables
q <- sdf_quantile(data_mutated_person_month_level, "pay", probabilities = c(0.0001, 0.999))
print(q)

# pull the 99.9% quantile
q2 <- q[2]

# check pay
check_pay <- get_summary_stats(data_mutated, "pay")
print(check_pay)

data_all <- data_mutated %>% 
  mutate(in_work = ifelse(pay > 0, 1, 0),
         pay_winsor = case_when(
         pay > q2 ~ q2,
         TRUE ~ pay))

data_all <- add_deflated_pay(data_all, pay_winsor)

# check pay
check_pay_winsor <- get_summary_stats(data_all, "pay_winsor")
print(check_pay_winsor)
check_pay_deflated <- get_summary_stats(data_all, "pay_deflated")
print(check_pay_deflated)


# check age
check_age_at_index_date <- get_summary_stats(data_all, "age_at_index_date")
print(check_age_at_index_date)
check_age_at_index_date_t0 <- get_summary_stats(data_all %>% filter(t_op == 0), "age_at_index_date")
print(check_age_at_index_date_t0)
check_age_month <- get_summary_stats(data_all, "age_month")
print(check_age_month)
check_age_month_t0 <- get_summary_stats(data_all %>% filter(t_op == 0), "age_month")
print(check_age_month_t0)


###
filter_list <- c("320", "170", "330", "120", 
                 "430", "301", "300", "100", 
                 "502", "400", "150", "130", 
                 "140", "160", "340", "410",
                 "110", "101", "710")


filter_list <- sort(filter_list)


data_all <- data_all %>% mutate(opertn_3_chr = str_sub(opertn_4_01, 1,3))


filter_list_lookup <- read.csv(lookup_tretspef, header = TRUE, stringsAsFactors = FALSE) %>%
  sparklyr::sdf_copy_to(sc, ., "tretspef_code_lookup", overwrite = TRUE)

data_all <- data_all %>%
  left_join(filter_list_lookup, by = c("tretspef" = "Code")) %>%
  rename(tretspef_description = Description)

# Get sample sizes for each treatment group - main
main_sample_sizes <- data_all %>% 
  filter(tretspef %in% filter_list) %>% 
  filter(t_op == 0) %>% 
  group_by(tretspef, tretspef_description) %>%
  summarise(sample_size = n()) %>%
  ungroup() %>%
  arrange(tretspef)

# Get sample sizes for each treatment group - stratified
strat_sample_sizes <- data_all %>% 
  filter(tretspef %in% filter_list) %>% 
  filter(t_op == 0) %>% 
  #filter(!is.na(elecdur)) %>% 
  group_by(flag_variable, tretspef, tretspef_description) %>%
  summarise(sample_size = n()) %>%
  ungroup() %>%
  arrange(tretspef, flag_variable)

# Run characteristics for main analysis
main_sample_age <- data_all %>% 
  filter(tretspef %in% filter_list) %>% 
  filter(t_op == 0) %>% 
  group_by(tretspef, tretspef_description, age_month_grouped) %>%
  summarise(sample_size = n()) %>%
  ungroup() %>%
  arrange(tretspef, age_month_grouped)

main_sample_sex <- data_all %>% 
  filter(tretspef %in% filter_list) %>% 
  filter(t_op == 0) %>% 
  group_by(tretspef, tretspef_description, sex) %>%
  summarise(sample_size = n()) %>%
  ungroup() %>%
  arrange(tretspef, sex)

main_sample_ethnicgrp <- data_all %>% 
  filter(tretspef %in% filter_list) %>% 
  filter(t_op == 0) %>% 
  group_by(tretspef, tretspef_description, ethnic_group) %>%
  summarise(sample_size = n()) %>%
  ungroup() %>%
  arrange(tretspef, ethnic_group)

# Get sample sizes for each treatment group - validation check - 01 September 2021 to 31 March 2022
validation_check <- data_all %>%
  filter(admidate >= "2021-09-01" & admidate <= "2022-03-31") %>%
  filter(tretspef %in% filter_list) %>% 
  filter(t_op == 0) %>% 
  group_by(tretspef, tretspef_description) %>%
  summarise(sample_size = n()) %>%
  ungroup() %>%
  arrange(tretspef)

# Save to excel file
desc_stats <- createWorkbook()

addWorksheet(desc_stats, "main_sample_sizes")
writeData(desc_stats, sheet = "main_sample_sizes", main_sample_sizes)

addWorksheet(desc_stats, "strat_sample_sizes")
writeData(desc_stats, sheet = "strat_sample_sizes", strat_sample_sizes)

addWorksheet(desc_stats, "main_sample_age")
writeData(desc_stats, sheet = "main_sample_age", main_sample_age)

addWorksheet(desc_stats, "main_sample_sex")
writeData(desc_stats, sheet = "main_sample_sex", main_sample_sex)

addWorksheet(desc_stats, "main_sample_ethnicgrp")
writeData(desc_stats, sheet = "main_sample_ethnicgrp", main_sample_ethnicgrp)

addWorksheet(desc_stats, "validation_check")
writeData(desc_stats, sheet = "validation_check", validation_check)

saveWorkbook(desc_stats, "SR_desc_stats.xlsx", overwrite = TRUE)
###


###
# Define function
run_analysis <- function(data, filter_list, analysis_name) {
  
  for (group in filter_list) {
    
    # Filter to specific group
    filtered_data <- data_all %>% filter(tretspef == group)
    
    # Step 1: Calculate proportion in work and average pay in each age group
    df_summary <- filtered_data %>%
      group_by(t_op, qrt_age) %>%
      summarise(
        count = n(),
        prop_in_work = sum(in_work)/n(),
        benefit_prop = sum(has_employment_support_benefit)/n(),
        average_pay = mean(pay_deflated),
        .groups = "drop"
        ) %>%
      group_by(t_op) %>%
      mutate(age_band_prop = count/sum(count)) %>%
      ungroup()

    # Step 2: Calculate overall age distribution at month t=0
    age_distribution <- df_summary %>%
      filter(t_op == 0) %>%
      select(qrt_age, age_band_prop) %>%
      rename(avg_prop = age_band_prop) %>%
      arrange(qrt_age)
    
    # Save the output to an Excel worksheet 
    age_distribution_save <- age_distribution %>%
      mutate(tretspef = group) %>%
      left_join(filter_list_lookup, by = c("tretspef" = "Code")) %>%
      rename(tretspef_description = Description) %>%
      relocate(tretspef_description, .before = everything()) %>%
      relocate(tretspef, .before = everything()) %>%
      arrange(qrt_age) %>%
      data.frame()
    if (group == filter_list[1]) {
      age_dist <- age_distribution_save
    } else {
      age_dist <- bind_rows(age_dist, age_distribution_save)
    }
    
    # Step 3: Calculate weighted average pay, employment, and weight using age distribution from month t=0
    weighted_df <- df_summary %>%
      left_join(age_distribution, by = "qrt_age") %>%
      group_by(t_op) %>%
      summarise(
        weighted_in_work = sum(prop_in_work * avg_prop),
        weighted_avg_pay = sum(average_pay * avg_prop),
        weighted_bene_prop = sum(benefit_prop * avg_prop),
        sample_size = sum(count),
        .groups = "drop") %>%
      arrange(t_op)
    
    # Save the output to an Excel worksheet 
    weighted_df_save <- weighted_df %>%
      mutate(tretspef = group) %>%
      left_join(filter_list_lookup, by = c("tretspef" = "Code")) %>%
      rename(tretspef_description = Description) %>%
      relocate(tretspef_description, .before = everything()) %>%
      relocate(tretspef, .before = everything()) %>%
      arrange(t_op) %>%
      data.frame()
    if (group == filter_list[1]) {
      main_results <- weighted_df_save
    } else {
      main_results <- bind_rows(main_results, weighted_df_save)
    }
   
    # Add stratification if flag_var is provided
    df_summary_strat <- filtered_data %>%
      #filter(!is.na(elecdur)) %>%
      #filter(elecdate != "1800-01-01" & elecdate != "1801-01-01") %>%
      #filter(elecdur <= 730) %>%
      group_by(flag_variable, t_op, qrt_age) %>%
      summarise(
        count = n(),
        prop_in_work = sum(in_work)/n(),
        benefit_prop = sum(has_employment_support_benefit)/n(),
        average_pay = mean(pay_deflated),
        .groups = "drop"
        ) %>%
      group_by(flag_variable, t_op) %>%
      mutate(age_band_prop = count/sum(count)) %>%
      ungroup()

    #  Calculate overall age distribution at month t=0
    age_distribution_strat <- df_summary_strat %>%
      filter(t_op == 0) %>%
      select(flag_variable, qrt_age, age_band_prop) %>%
      rename(avg_prop = age_band_prop) %>%
      arrange(flag_variable, qrt_age)
    
    # Save the output to an Excel worksheet 
    age_distribution_strat_save <- age_distribution_strat %>%
      mutate(tretspef = group) %>%
      left_join(filter_list_lookup, by = c("tretspef" = "Code")) %>%
      rename(tretspef_description = Description) %>%
      relocate(tretspef_description, .before = everything()) %>%
      relocate(tretspef, .before = everything()) %>%
      arrange(flag_variable, qrt_age) %>%
      data.frame()    
    if (group == filter_list[1]) {
      age_dist_strat <- age_distribution_strat_save
    } else {
      age_dist_strat <- bind_rows(age_dist_strat, age_distribution_strat_save)
    }

    df_weighted_strat <- df_summary_strat %>%
      left_join(age_distribution_strat, by = c("flag_variable", "qrt_age")) %>%
      group_by(flag_variable, t_op) %>%
      summarise(
        weighted_in_work = sum(prop_in_work * avg_prop),
        weighted_avg_pay= sum(average_pay * avg_prop),
        weighted_bene_prop = sum(benefit_prop * avg_prop),
        sample_size = sum(count),
        .groups = "drop") %>%
      arrange(flag_variable, t_op)

    # Save the output to an Excel worksheet 
    df_weighted_strat_save <- df_weighted_strat %>%
      mutate(tretspef = group) %>%
      left_join(filter_list_lookup, by = c("tretspef" = "Code")) %>%
      rename(tretspef_description = Description) %>%
      relocate(tretspef_description, .before = everything()) %>%
      relocate(tretspef, .before = everything()) %>%
      arrange(flag_variable, t_op) %>%
      data.frame()    
    if (group == filter_list[1]) {
      strat_results <- df_weighted_strat_save
    } else {
      strat_results <- bind_rows(strat_results, df_weighted_strat_save)
    }
    
  }
  
  # Write main results to Excel  
  main_wb <- createWorkbook()
              
  addWorksheet(main_wb, "main_age_dist")
  writeData(main_wb, "main_age_dist", age_dist) 
  
  addWorksheet(main_wb, "main_output")
  writeData(main_wb, "main_output", main_results)           

  addWorksheet(main_wb, "stratified_age_dist")
  writeData(main_wb, "stratified_age_dist", age_dist_strat)           

  addWorksheet(main_wb, "stratified_output")
  writeData(main_wb, "stratified_output", strat_results)        
          
  saveWorkbook(main_wb, paste0("age_std_output_", analysis_name, ".xlsx"), overwrite = TRUE)

}


### Run tables ###

#filter_list <- c("100")

# 1. Monthly data, all patients

run_analysis(data_all, filter_list, "overall_3004")


# 2. Monthly data, people waiting, referral to end of study

on_wt_list <- data_all %>%
 mutate(ref_date_ym = substr(elecdate, 1, 7)) %>%
 mutate(trt_date_ym = substr(admidate, 1, 7)) %>%

 mutate(elecdate = as.Date(elecdate)) %>%
 mutate(ref_date_ym = as.Date(ref_date_ym)) %>%
 mutate(trt_date_ym = as.Date(trt_date_ym)) %>%

 filter(month_start >= ref_date_ym)

# check t_op
check_on_wt_list <- get_summary_stats(on_wt_list, "t_op")
print(check_on_wt_list)

run_analysis(on_wt_list, filter_list, "wt_end_of_study")


# 3. Monthly data, people waiting, referral to admission

on_wt_list_only <- on_wt_list %>%

 filter(month_start <= trt_date_ym)

# check t_op
check_on_wt_list_only <- get_summary_stats(on_wt_list_only, "t_op")
print(check_on_wt_list_only)

run_analysis(on_wt_list_only, filter_list, "wt_only")



# Most common procedure codes and diag codes for each tretspef



# Example function with fixed variable names, separate tabs, and code column using openxlsx
treatment_counts <- function(df, codes, output_file) {
  # Initialize lists to store results
  var1_counts_list <- list()
  var2_counts_list <- list()
  var3_counts_list <- list()
  
  # Loop through each code and perform the filtering and counting
  for (code in filter_list) {
    filtered_df <- df %>%
      filter(t_op == 0) %>%
      filter(tretspef == code)
    
    # Count occurrences of var1
    counts_diagnosis <- filtered_df %>%
      group_by(diag_4_01) %>%
      summarise(count = n()) %>%
      filter(count >= 10) %>%
      arrange(desc(count)) %>%
      head(30) %>%
      mutate(code = code) %>%
      collect()
    
    # Count occurrences of var2
    counts_opcs <- filtered_df %>%
      group_by(opertn_4_01) %>%
      summarise(count = n()) %>%
      filter(count >= 10) %>%
      arrange(desc(count)) %>%
      head(30) %>%
      mutate(code = code) %>%
      collect()
    
    counts_opcs3chr <- filtered_df %>%
      group_by(opertn_3_chr) %>%
      summarise(count = n()) %>%
      filter(count >= 10) %>%
      arrange(desc(count)) %>%
      head(30) %>%
      mutate(code = code) %>%
      collect()
    
    # Append results to lists
    var1_counts_list[[code]] <- counts_diagnosis
    var2_counts_list[[code]] <- counts_opcs
    var3_counts_list[[code]] <- counts_opcs3chr
  }
  
  # Combine all results into data frames
  var1_counts_df <- bind_rows(var1_counts_list)
  var2_counts_df <- bind_rows(var2_counts_list)
  var3_counts_df <- bind_rows(var3_counts_list)
  
  # Create a new workbook
  wb <- createWorkbook()
  
  # Add worksheets for diag and opcs counts
  addWorksheet(wb, "Diagnosis Counts")
  addWorksheet(wb, "OPCS Counts")
  addWorksheet(wb, "OPCS 3 Chr Counts")
  
  # Write data to worksheets
  writeData(wb, "Diagnosis Counts", var1_counts_df)
  writeData(wb, "OPCS Counts", var2_counts_df)
  writeData(wb, "OPCS 3 Chr Counts", var3_counts_df)
  
  # Save the workbook to an Excel file
  saveWorkbook(wb, output_file, overwrite = TRUE)
}

# Use function
treatment_counts(data_all, filter_list, "diagnosis and treatment freqs.xlsx")



