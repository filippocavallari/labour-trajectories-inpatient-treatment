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


number_to_name <- c("320" = "Cardiology Service - heart disease patients, intervention, prevention and diagnostics", 
                    "170" = "Cardiothoracic Surgery Service - surgical treatment heart & chest", 
                    "330" = "Dermatology Service - diagnosis, management and treatment of skin diseases",
                    "120" = "Ear Nose and Throat Service - as listed but excludes audiology", 
                    "430" = "Elderly Medicine Service - treatment for older adults/people with comorbidities, no set age", 
                    "301" = "Gastroenterology Service - screening, diagnostic and therapeutic for upper and lower GI",
                    "300" = "General Internal Medicine Service - acute problems, multiple disorders and no other obvious category", 
                    "100" = "General Surgery Service - anything that doesn't have specific category roughly 80% don't have a specialty", 
                    "502" = "Gynaecology Service - female reproductive system, includes planned termination of pregnancy",
                    "400" = "Neurology Service - neurological conditions, excludes stroke or transient ischaemic attack", 
                    "150" = "Neurosurgical Service - nervous system, brain, spinal cord, peripheral nerves, excluded spinal and trauma surgery", 
                    "130" = "Ophthalmology Service - diseases of the eye", 
                    "140" = "Oral Surgery Service - diseases, injuries and defects of the mouth and soft tissues", 
                    "160" = "Plastic Surgery Service - correct or restore form or function, reconstructive and cosmetic", 
                    "340" = "Respiratory Medicine Service - respiratory complaints, excluding ARDS",
                    "410" = "Rheumatology Service - investigation, management and rehab of MSK disorders and blood vessels", 
                    "110" = "Trauma and Orthopaedic Service - treat injuries, congenital and acquired disorders of bone, joints, soft tissues etc", 
                    "101" = "Urology Service - urinary system and male reproductive, may include surgery for gender dysphoria",
                    "710" = "Adult Mental Health Service - assessment, diagnosis, treatment and maintenance")

# Define function
process_excel_sheets <- function(file_path, sheet_name, output_folder, output_excel_file, 
                                 output_excel_file_cumsum, output_folder_cumsum, output_pdf_file,
                                 three_months, four_months) {
  
  age_std_output <- read_excel(file_path, sheet = sheet_name) 
  
  tretspef_codes <- age_std_output %>%
    select(tretspef) %>%
    distinct() %>%
    arrange(tretspef) %>%
    pull()
  
  results <- list()
  
  cumsum <- list()
  
  pdf(output_pdf_file)
  
  for (i in tretspef_codes) {
    
    age_std_output_i <- age_std_output %>%
      filter(tretspef == i) %>%
      rename(pay = weighted_avg_pay) %>%
      rename(employment = weighted_in_work) %>%
      rename(benefits = weighted_bene_prop)
    
    if (i %in% three_months) {
    model_pay <- lm(pay ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -3 & t_op < 0))
    model_employment <- glm(employment ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -3 & t_op < 0), family = "binomial")
    model_benefits <- glm(benefits ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -3 & t_op < 0), family = "binomial")
    
    age_std_output_i <- age_std_output_i %>%
      mutate(pred_pay = predict(model_pay, age_std_output_i),
             pred_pay = ifelse(pred_pay < 0, 0, pred_pay),
             effect_pay = pay - pred_pay,
             pred_employment = predict(model_employment, age_std_output_i, type = "response"),
             pred_employment = ifelse(pred_employment < 0, 0, pred_employment),
             effect_employment = employment - pred_employment,
             pred_benefits = predict(model_benefits, age_std_output_i, type = "response"),
             pred_benefits = ifelse(pred_benefits < 0, 0, pred_benefits),
             effect_benefits = benefits - pred_benefits,
             training_months = 3)
    
    total_effect_pay = format(round(sum(filter(age_std_output_i, t_op >= 0)$effect_pay)), big.mark = ",")
    
    pay_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = pay) +
      geom_point() + 
      geom_line(aes(y = pred_pay), data = filter(age_std_output_i, t_op >= -3)) + 
      labs(
        x = "Time to / since treatment",
        y = "Monthly employee pay",
        title = str_wrap(number_to_name[i], width = 60),  
        subtitle = paste0("Total effect: ", total_effect_pay)
      ) +
      theme_bw() +
      scale_y_continuous(labels = scales::comma, limits=c(0, 3000))
    
    #total_effect_emp = round(sum(age_std_output_i$effect_employment))
    employment_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = employment) +
      geom_point() + 
      geom_line(aes(y = pred_employment), data = filter(age_std_output_i, t_op >= -3)) +
      labs(
        x = "Time to / since treatment",
        y = "Probability of being in paid employment",
        title = str_wrap(number_to_name[i], width = 60)
        #subtitle = paste0("Total effect: ", total_effect_emp)
      ) +
      theme_bw()
    
    #total_effect_benefits = round(sum(age_std_output_i$effect_benefits))
    benefits_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = benefits) +
      geom_point() + 
      geom_line(aes(y = pred_benefits), data = filter(age_std_output_i, t_op >= -3)) +
      labs(
        x = "Time to / since treatment",
        y = "Probability of receiving any benefit",
        title = str_wrap(number_to_name[i], width = 60)
        #subtitle = paste0("Total effect: ", total_effect_benefits)
      ) +
      theme_bw()
      
  } else if (i %in% four_months) {
      
    model_pay <- lm(pay ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -4 & t_op < 0))
    model_employment <- glm(employment ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -4 & t_op < 0), family = "binomial")
    model_benefits <- glm(benefits ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -4 & t_op < 0), family = "binomial")
    
    age_std_output_i <- age_std_output_i %>%
      mutate(pred_pay = predict(model_pay, age_std_output_i),
             pred_pay = ifelse(pred_pay < 0, 0, pred_pay),
             effect_pay = pay - pred_pay,
             pred_employment = predict(model_employment, age_std_output_i, type = "response"),
             pred_employment = ifelse(pred_employment < 0, 0, pred_employment),
             effect_employment = employment - pred_employment,
             pred_benefits = predict(model_benefits, age_std_output_i, type = "response"),
             pred_benefits = ifelse(pred_benefits < 0, 0, pred_benefits),
             effect_benefits = benefits - pred_benefits,
             training_months = 4)
    
    total_effect_pay = format(round(sum(filter(age_std_output_i, t_op >= 0)$effect_pay)), big.mark = ",")
    
    pay_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = pay) +
      geom_point() + 
      geom_line(aes(y = pred_pay), data = filter(age_std_output_i, t_op >= -4)) + 
      labs(
        x = "Time to / since treatment",
        y = "Monthly employee pay",
        title = str_wrap(number_to_name[i], width = 60),  
        subtitle = paste0("Total effect: ", total_effect_pay)
      ) +
      theme_bw() +
      scale_y_continuous(labels = scales::comma, limits=c(0, 3000))
    
    #total_effect_emp = round(sum(age_std_output_i$effect_employment))
    employment_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = employment) +
      geom_point() + 
      geom_line(aes(y = pred_employment), data = filter(age_std_output_i, t_op >= -4)) +
      labs(
        x = "Time to / since treatment",
        y = "Probability of being in paid employment",
        title = str_wrap(number_to_name[i], width = 60)
        #subtitle = paste0("Total effect: ", total_effect_emp)
      ) +
      theme_bw()
    
    #total_effect_benefits = round(sum(age_std_output_i$effect_benefits))
    benefits_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = benefits) +
      geom_point() + 
      geom_line(aes(y = pred_benefits), data = filter(age_std_output_i, t_op >= -4)) +
      labs(
        x = "Time to / since treatment",
        y = "Probability of receiving any benefit",
        title = str_wrap(number_to_name[i], width = 60)
        #subtitle = paste0("Total effect: ", total_effect_benefits)
      ) +
      theme_bw()
      
    } else {
      
        model_pay <- lm(pay ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -6 & t_op < 0))
    model_employment <- glm(employment ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -6 & t_op < 0), family = "binomial")
    model_benefits <- glm(benefits ~ poly(t_op, 1), data = filter(age_std_output_i, t_op >= -6 & t_op < 0), family = "binomial")
    
    age_std_output_i <- age_std_output_i %>%
      mutate(pred_pay = predict(model_pay, age_std_output_i),
             pred_pay = ifelse(pred_pay < 0, 0, pred_pay),
             effect_pay = pay - pred_pay,
             pred_employment = predict(model_employment, age_std_output_i, type = "response"),
             pred_employment = ifelse(pred_employment < 0, 0, pred_employment),
             effect_employment = employment - pred_employment,
             pred_benefits = predict(model_benefits, age_std_output_i, type = "response"),
             pred_benefits = ifelse(pred_benefits < 0, 0, pred_benefits),
             effect_benefits = benefits - pred_benefits,
             training_months = 6)
    
    total_effect_pay = format(round(sum(filter(age_std_output_i, t_op >= 0)$effect_pay)), big.mark = ",")
    
    pay_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = pay) +
      geom_point() + 
      geom_line(aes(y = pred_pay), data = filter(age_std_output_i, t_op >= -6)) + 
      labs(
        x = "Time to / since treatment",
        y = "Monthly employee pay",
        title = str_wrap(number_to_name[i], width = 60),  
        subtitle = paste0("Total effect: ", total_effect_pay)
      ) +
      theme_bw() +
      scale_y_continuous(labels = scales::comma, limits=c(0, 3000))
    
    #total_effect_emp = round(sum(age_std_output_i$effect_employment))
    employment_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = employment) +
      geom_point() + 
      geom_line(aes(y = pred_employment), data = filter(age_std_output_i, t_op >= -6)) +
      labs(
        x = "Time to / since treatment",
        y = "Probability of being in paid employment",
        title = str_wrap(number_to_name[i], width = 60)
        #subtitle = paste0("Total effect: ", total_effect_emp)
      ) +
      theme_bw()
    
    #total_effect_benefits = round(sum(age_std_output_i$effect_benefits))
    benefits_plot <- ggplot(age_std_output_i) + 
      aes(x = t_op, y = benefits) +
      geom_point() + 
      geom_line(aes(y = pred_benefits), data = filter(age_std_output_i, t_op >= -6)) +
      labs(
        x = "Time to / since treatment",
        y = "Probability of receiving any benefit",
        title = str_wrap(number_to_name[i], width = 60)
        #subtitle = paste0("Total effect: ", total_effect_benefits)
      ) +
      theme_bw()
      
    }
    
    print(pay_plot)
    print(employment_plot)
    print(benefits_plot)
    
    ggsave(filename = paste0(output_folder, "/", i, "_pay_plot.png"), plot = pay_plot)
    ggsave(filename = paste0(output_folder, "/", i, "_employment_plot.png"), plot = employment_plot)
    ggsave(filename = paste0(output_folder, "/", i, "_benefits_plot.png"), plot = benefits_plot)
    
    pay_cumsum <- age_std_output_i %>%
      filter(t_op >= 0) %>%
      mutate(cumul_effect_pay = cumsum(effect_pay))
    
    pay_plot_cumsum <- ggplot(pay_cumsum) + 
      aes(x = t_op, 
          y = cumul_effect_pay) +
      geom_point() +
      theme_bw()
    
    ggsave(filename = paste0(output_folder_cumsum, "/", i, "_cumsum_pay_plot.png"), plot = pay_plot_cumsum)
    
    results[[i]] <- age_std_output_i
    
    cumsum[[i]] <- pay_cumsum
    
  }
  
  dev.off()
  
  comb_results <- bind_rows(results)
  comb_cumsum <- bind_rows(cumsum)

  # Save each modified data frame to an Excel file with multiple sheets
  write_xlsx(comb_results, path = output_excel_file)
  
  write_xlsx(comb_cumsum, path = output_excel_file_cumsum)
  
  return(comb_results)
  return(comb_cumsum)
  
}



project_folder <- paste0("Prediction output")

file_path <- paste0(project_folder, "/age_std_output_overall_3004.xlsx")
sheet_name <- "main_output"

output_folder <- paste0(project_folder, "/Prediction plots")
output_folder_cumsum <- paste0(project_folder, "/Cumulative sum plots")

output_excel_file <- paste0(output_folder, "/prediction_data.xlsx")
output_excel_file_cumsum <- paste0(output_folder_cumsum, "/effect_on_pay.xlsx")

output_pdf_file <- paste0(output_folder, "/prediction_plots_pdf.pdf")
  
three_months <- c("110", "150", "160", "502")

four_months <- c("100", "170", "340")
  

# Run functions
results <- process_excel_sheets(file_path, sheet_name, output_folder, output_excel_file, output_excel_file_cumsum, 
                                output_folder_cumsum, output_pdf_file, three_months, four_months)
