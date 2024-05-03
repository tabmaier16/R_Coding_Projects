#Load Packages
#library(beepr)
#library(RODBC)
#library(odbc)
library(tidyverse)
library(readxl)
library(writexl)
library(lubridate)
library(DBI)
library(glue)
library(dplyr)
library(plyr)
library(tidyr)
library(readr)
library(gapminder)

#Read in Weekly report
weekly_report <- read.csv("//BARRACUDA/SLS_Depts/DEPTS/Mass Channel/WALMART/ED CARDS_Current Year/POS REPORTS/Dashboard/FY25 (2024)/Data/Weekly24_by Store by Subcat L52Wk L4Wks and LW.csv")

#Create data frame
weekly_report_df <- as.data.frame(weekly_report)

#Rename Wm_time_window_week to time_period since we'll eventually union this data with FYTD data
weekly_report_df <-
  weekly_report_df %>% dplyr::rename(time_period = wm_time_window_week)

#Remove Dollar Signs and Commas from Sales Metrics
weekly_report_df$pos_bis_sales_this_year <- gsub("\\$|,", "", weekly_report_df$pos_bis_sales_this_year)
weekly_report_df$pos_bis_sales_last_year <- gsub("\\$|,", "", weekly_report_df$pos_bis_sales_last_year)
weekly_report_df$pos_bis_quantity_this_year <- gsub("\\$|,", "", weekly_report_df$pos_bis_quantity_this_year)
weekly_report_df$pos_bis_quantity_last_year <- gsub("\\$|,", "", weekly_report_df$pos_bis_quantity_last_year)

weekly_report_df$pos_delivery_sales_this_year <- gsub("\\$|,", "", weekly_report_df$pos_delivery_sales_this_year)
weekly_report_df$pos_delivery_sales_last_year <- gsub("\\$|,", "", weekly_report_df$pos_delivery_sales_last_year)
weekly_report_df$pos_delivery_quantity_this_year <- gsub("\\$|,", "", weekly_report_df$pos_delivery_quantity_this_year)
weekly_report_df$pos_delivery_quantity_last_year <- gsub("\\$|,", "", weekly_report_df$pos_delivery_quantity_last_year)

weekly_report_df$pos_pickup_sales_this_year <- gsub("\\$|,", "", weekly_report_df$pos_pickup_sales_this_year)
weekly_report_df$pos_pickup_sales_last_year <- gsub("\\$|,", "", weekly_report_df$pos_pickup_sales_last_year)
weekly_report_df$pos_pickup_quantity_this_year <- gsub("\\$|,", "", weekly_report_df$pos_pickup_quantity_this_year)
weekly_report_df$pos_pickup_quantity_last_year <- gsub("\\$|,", "", weekly_report_df$pos_pickup_quantity_last_year)

weekly_report_df$pos_sfs_sales_this_year <- gsub("\\$|,", "", weekly_report_df$pos_sfs_sales_this_year)
weekly_report_df$pos_sfs_sales_last_year <- gsub("\\$|,", "", weekly_report_df$pos_sfs_sales_last_year)
weekly_report_df$pos_sfs_quantity_this_year <- gsub("\\$|,", "", weekly_report_df$pos_sfs_quantity_this_year)
weekly_report_df$pos_sfs_quantity_last_year <- gsub("\\$|,", "", weekly_report_df$pos_sfs_quantity_last_year)

#Mutate data frame so that all the Sales metrics are numeric variables
weekly_report_df <- weekly_report_df %>% mutate_at(vars(38:53), as.numeric)

#Split Weekly report into 2 pieces: OPD Metrics and Sales Metrics
#Splitting into 2 pieces to avoid quadrupling of OPD Metrics when pivoting Sales Metrics
#We will NOT pivot the OPD Metrics
#We WILL pivot the Sales Metrics

weekly_opd_metrics <- weekly_report_df %>% select(c(1:37))
weekly_sales_metrics <- weekly_report_df %>% select(-c(22:37))

#Using Weekly Sales Metrics, pivot all Sales Metrics
weekly_sales_metrics_2 <-
  weekly_sales_metrics %>% pivot_longer(cols = 22:37,
                                        names_to = "Sales_Metric_Name",
                                        values_to = "Sales_Metric_Value")

#Mutate to create Channel field for BIS, Delivery, Pickup, and SFS
weekly_sales_metrics_3 <-
  weekly_sales_metrics_2 %>% mutate(
    Channel =
      case_when(
        grepl("bis", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "BIS",
        grepl("delivery", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "Delivery",
        grepl("pickup", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "Pickup",
        grepl("sfs", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "SFS",
      )
  )

#Mutate to create Simplified Metric field for TY Sales, LY Sales, TY Quantity, LY Quantity
weekly_sales_metrics_4 <-
  weekly_sales_metrics_3 %>% mutate(
    Metric =
      case_when(
        grepl("sales_this_year", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "TY_Sales",
        grepl("sales_last_year", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "LY_Sales",
        grepl("quantity_this_year", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "TY_Quantity",
        grepl("quantity_last_year", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "LY_Quantity",
      )
  )

#Drop Sales_Metric_Name
weekly_sales_metrics_5 <-
  weekly_sales_metrics_4 %>% select(-c(Sales_Metric_Name))

#Unpivot Metrics so we have 4 separate columns for TY Sales, LY Sales, TY Quantity, LY Quantity
weekly_sales_metrics_6 <-
  weekly_sales_metrics_5 %>% pivot_wider(names_from = Metric,
                                         values_from = Sales_Metric_Value)

#Read in FYTD report
fytd_report <- read.csv("//BARRACUDA/SLS_Depts/DEPTS/Mass Channel/WALMART/ED CARDS_Current Year/POS REPORTS/Dashboard/FY25 (2024)/Data/Weekly24_by Store by Subcat FTYD.csv")

#Create data frame
fytd_report_df <- as.data.frame(fytd_report)

#Add column for time_period before omni_category_description and fill with FYTD
fytd_report_df_2 <-
  fytd_report_df %>% mutate("time_period" = "FYTD") %>% select(time_period, everything())

#Remove Dollar Signs and Commas from Sales Metrics
fytd_report_df_2$pos_bis_sales_this_year <- gsub("\\$|,", "", fytd_report_df_2$pos_bis_sales_this_year)
fytd_report_df_2$pos_bis_sales_last_year <- gsub("\\$|,", "", fytd_report_df_2$pos_bis_sales_last_year)
fytd_report_df_2$pos_bis_quantity_this_year <- gsub("\\$|,", "", fytd_report_df_2$pos_bis_quantity_this_year)
fytd_report_df_2$pos_bis_quantity_last_year <- gsub("\\$|,", "", fytd_report_df_2$pos_bis_quantity_last_year)

fytd_report_df_2$pos_delivery_sales_this_year <- gsub("\\$|,", "", fytd_report_df_2$pos_delivery_sales_this_year)
fytd_report_df_2$pos_delivery_sales_last_year <- gsub("\\$|,", "", fytd_report_df_2$pos_delivery_sales_last_year)
fytd_report_df_2$pos_delivery_quantity_this_year <- gsub("\\$|,", "", fytd_report_df_2$pos_delivery_quantity_this_year)
fytd_report_df_2$pos_delivery_quantity_last_year <- gsub("\\$|,", "", fytd_report_df_2$pos_delivery_quantity_last_year)

fytd_report_df_2$pos_pickup_sales_this_year <- gsub("\\$|,", "", fytd_report_df_2$pos_pickup_sales_this_year)
fytd_report_df_2$pos_pickup_sales_last_year <- gsub("\\$|,", "", fytd_report_df_2$pos_pickup_sales_last_year)
fytd_report_df_2$pos_pickup_quantity_this_year <- gsub("\\$|,", "", fytd_report_df_2$pos_pickup_quantity_this_year)
fytd_report_df_2$pos_pickup_quantity_last_year <- gsub("\\$|,", "", fytd_report_df_2$pos_pickup_quantity_last_year)

fytd_report_df_2$pos_sfs_sales_this_year <- gsub("\\$|,", "", fytd_report_df_2$pos_sfs_sales_this_year)
fytd_report_df_2$pos_sfs_sales_last_year <- gsub("\\$|,", "", fytd_report_df_2$pos_sfs_sales_last_year)
fytd_report_df_2$pos_sfs_quantity_this_year <- gsub("\\$|,", "", fytd_report_df_2$pos_sfs_quantity_this_year)
fytd_report_df_2$pos_sfs_quantity_last_year <- gsub("\\$|,", "", fytd_report_df_2$pos_sfs_quantity_last_year)

#Mutate data frame so that all the Sales metrics are numeric variables
fytd_report_df_2 <- fytd_report_df_2 %>% mutate_at(vars(38:53), as.numeric)

#Splitting into 2 pieces to avoid quadrupling of OPD Metrics when pivoting Sales Metrics
fytd_opd_metrics <- fytd_report_df_2 %>% select(c(1:37))
fytd_sales_metrics <- fytd_report_df_2 %>% select(-c(22:37))

#Using FYTD Sales Metrics, pivot all Sales Metrics
fytd_sales_metrics_2 <-
  fytd_sales_metrics %>% pivot_longer(cols = 22:37,
                                      names_to = "Sales_Metric_Name",
                                      values_to = "Sales_Metric_Value")

#Mutate to create Channel field for BIS, Delivery, Pickup, and SFS
fytd_sales_metrics_3 <- fytd_sales_metrics_2 %>% mutate(
  Channel =
    case_when(
      grepl("bis", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "BIS",
      grepl("delivery", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "Delivery",
      grepl("pickup", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "Pickup",
      grepl("sfs", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "SFS",
    )
)

#Mutate to create Simplified Metric field for TY Sales, LY Sales, TY Quantity, LY Quantity
fytd_sales_metrics_4 <- fytd_sales_metrics_3 %>% mutate(
  Metric =
    case_when(
      grepl("sales_this_year", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "TY_Sales",
      grepl("sales_last_year", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "LY_Sales",
      grepl("quantity_this_year", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "TY_Quantity",
      grepl("quantity_last_year", Sales_Metric_Name, fixed = TRUE) == TRUE ~ "LY_Quantity",
    )
)

#Drop Sales_Metric_Name
fytd_sales_metrics_5 <-
  fytd_sales_metrics_4 %>% select(-c(Sales_Metric_Name))

#Unpivot Metrics so we have 4 separate columns for TY Sales, LY Sales, TY Quantity, LY Quantity
fytd_sales_metrics_6 <-
  fytd_sales_metrics_5 %>% pivot_wider(names_from = Metric,
                                       values_from = Sales_Metric_Value)

#Union OPD Metrics Tables
total_opd_metrics <- union_all(weekly_opd_metrics, fytd_opd_metrics)

#Union Sales Metrics Tables
total_sales_metrics <-
  union_all(weekly_sales_metrics_6, fytd_sales_metrics_6)

#Read in file with Go Forward Position information, trim to remove unused columns
gf_store_groups <-
  read_xlsx(
    "//BARRACUDA/SLS_Depts/DEPTS/Mass Channel/WALMART/ED CARDS_Current Year/POS REPORTS/Dashboard/FY25 (2024)/Code/Lookup_Tables/Full Store List with Go Forward Position 10.26.23.xlsx",
    sheet = 1,
#    col_types = 'text'
  )

gf_store_groups_clean <-
  gf_store_groups %>% select(c("Store Id", "Go Forward Disposition"))

#Add Store Group information to OPD Metrics
total_opd_metrics <- total_opd_metrics %>% mutate_at(vars(9), as.numeric)

final_opd_metrics <-
  total_opd_metrics %>% left_join(gf_store_groups_clean, by = c("store_number" = "Store Id"))

#Aggregate OPD Metrics to Banner + Store Group level
final_opd_metrics_2 <-
  final_opd_metrics %>% group_by(
    time_period,
    omni_category_description,
    omni_category_group_description,
    omni_category_group_number,
    omni_department_description,
    omni_department_number,
    omni_subcategory_description,
    vendor_number,
    banner_description,
    business_company_description,
    `Go Forward Disposition`
  ) %>%
  dplyr::summarise(
    first_time_pick_rate_denominator = sum(first_time_pick_rate_denominator),
    first_time_pick_rate_numerator = sum(first_time_pick_rate_numerator),
    first_time_pick_rate_quantity = sum(first_time_pick_rate_quantity),
    nil_picks = sum(nil_picks),
    customer_order_quantity = sum(customer_order_quantity),
    post_substitute_rate_denominator = sum(post_substitute_rate_denominator),
    post_substitute_rate_denominator = sum(post_substitute_rate_denominator),
    post_substitute_rate_numerator = sum(post_substitute_rate_numerator),
    pre_substitute_quantity = sum(pre_substitute_quantity),
    pre_substitute_rate_denominator = sum(pre_substitute_rate_denominator),
    pre_substitute_rate_numerator = sum(pre_substitute_rate_numerator),
    scheduled_nil_pick_quantity = sum(scheduled_nil_pick_quantity),
    scheduled_nil_pick_rate_denominator = sum(scheduled_nil_pick_rate_denominator),
    scheduled_nil_pick_rate_numerator = sum(scheduled_nil_pick_rate_numerator),
    unscheduled_nil_pick_quantity = sum(unscheduled_nil_pick_quantity),
    unscheduled_nil_pick_rate_denominator = sum(unscheduled_nil_pick_rate_denominator),
    unscheduled_nil_pick_rate_numerator = sum(unscheduled_nil_pick_rate_numerator)
  )

#Output final OPD Metrics file
writexl::write_xlsx(final_opd_metrics_2,
                    path = "//BARRACUDA/SLS_Depts/DEPTS/Mass Channel/WALMART/ED CARDS_Current Year/POS REPORTS/Dashboard/FY25 (2024)/Data/weekly_and_fytd_opd_metrics_csv.xlsx",
                    col_names = TRUE)

#Add Store Group information to Sales Metrics
total_sales_metrics <- total_sales_metrics %>% mutate_at(vars(9), as.numeric)

final_sales_metrics <-
  total_sales_metrics %>% left_join(gf_store_groups_clean, by = c("store_number" = "Store Id"))

#Aggregate Sales Metrics to Banner + Store Group level
final_sales_metrics_2 <-
  final_sales_metrics %>% group_by(
    time_period,
    omni_category_description,
    omni_category_group_description,
    omni_category_group_number,
    omni_department_description,
    omni_department_number,
    omni_subcategory_description,
    vendor_number,
    banner_description,
    business_company_description,
    Channel,
    `Go Forward Disposition`
  ) %>%
  dplyr::summarise(
    TY_Sales = sum(TY_Sales),
    LY_Sales = sum(LY_Sales),
    TY_Quantity = sum(TY_Quantity),
    LY_Quantity = sum(LY_Quantity)
  )

#Read in S2H 1P file
s2h_1p_report <- read.csv("//BARRACUDA/SLS_Depts/DEPTS/Mass Channel/WALMART/ED CARDS_Current Year/POS REPORTS/Dashboard/FY25 (2024)/Data/Weekly24_1P_saved_as_csv.csv")

s2h_1p_report_df <- as.data.frame(s2h_1p_report)


#Subset the table to only include desired Dimensions and the 4 "Shipped Based" metrics
s2h_1p_report_df <- s2h_1p_report_df %>% select(c(1, 3:8, 50:54))
#Shifted last column references left by 1 because "ecomm_prod_id" was missing from reports on 2.19.24
#s2h_1p_report_df <- s2h_1p_report_df %>% select(c(1, 3:8, 49:53))


#Rename metrics to TY_Sales, LY_Sales, TY_Quantity, LY_Quantity
s2h_1p_report_df <-
  s2h_1p_report_df %>% dplyr::rename(
    TY_Sales = shipped_based_net_sales_amount_this_year,
    LY_Sales = shipped_based_net_sales_amount_last_year,
    TY_Quantity = shipped_based_quantity_this_year,
    LY_Quantity = shipped_based_quantity_last_year
  )

#Remove Dollar Signs and Commas from Sales Metrics
s2h_1p_report_df$TY_Sales <- gsub("\\$|,", "", s2h_1p_report_df$TY_Sales)
s2h_1p_report_df$LY_Sales <- gsub("\\$|,", "", s2h_1p_report_df$LY_Sales)
s2h_1p_report_df$TY_Quantity <- gsub("\\$|,", "", s2h_1p_report_df$TY_Quantity)
s2h_1p_report_df$LY_Quantity <- gsub("\\$|,", "", s2h_1p_report_df$LY_Quantity)

#Mutate data frame so that all the Sales metrics are numeric variables
s2h_1p_report_df <- s2h_1p_report_df %>% mutate_at(vars(9:12), as.numeric)

#Insert columns after vendor_number for:
#banner_description: S2H 1P
#business_company_description: S2H 1P
#Channel: S2H 1P
#Go Forward Disposition: S2H 1P

s2h_1p_report_df_2 <-
  s2h_1p_report_df %>% mutate(
    "banner_description" = "S2H 1P",
    "business_company_description" = "S2H 1P",
    "Channel" = "S2H 1P",
    "Go Forward Disposition" = "S2H 1P"
  )


#Read in VG's file for WMT Weeks -> Actual Dates
wmt_week_lookup <-
  read_xlsx(
    "//BARRACUDA/SLS_Depts/DEPTS/Mass Channel/WALMART/ED CARDS_Current Year/POS REPORTS/Dashboard/FY25 (2024)/Code/Lookup_Tables/Calendar Conversion for 1p.xlsx",
    sheet = 1,
  )

#Convert Walmart Calendar Week from numeric to text for join
wmt_week_lookup_2 <-
  wmt_week_lookup %>% mutate("Walmart Calendar Week" = as.integer(`Walmart Calendar Week`))


#Join S2H 1P data with the Dates file to implement date logic
s2h_1p_report_df_3 <-
  s2h_1p_report_df_2 %>% left_join(wmt_week_lookup_2,
                                   by = c("walmart_calendar_week" = "Walmart Calendar Week"))


#Calculate max date in data set for date logic
max_date <- max(s2h_1p_report_df_3$"Start Date", na.rm = TRUE)


#Write logic to isolate LW data
#Add time_period field and populate with "Last week"
s2h_1p_lw <- s2h_1p_report_df_3 %>% filter(`Start Date` == max_date)

s2h_1p_lw <-
  s2h_1p_lw %>% mutate("time_period" = "Last week") %>% select(time_period, everything())


#Write logic to isolate L4W
#Add time_period field and populate with "Last 4 weeks"
s2h_1p_l4w <-
  s2h_1p_report_df_3 %>% filter(`Start Date` > max_date - weeks(4))

s2h_1p_l4w <-
  s2h_1p_l4w %>% mutate("time_period" = "Last 4 weeks") %>% select(time_period, everything())


#Write logic to isolate L52W
#Add time_period field and populate with "Last 52 weeks"
s2h_1p_l52w <-
  s2h_1p_report_df_3 %>% filter(`Start Date` > max_date - weeks(52))

s2h_1p_l52w <-
  s2h_1p_l52w %>% mutate("time_period" = "Last 52 weeks") %>% select(time_period, everything())


#Write logic to isolate FYTD
#Add time_period field and populate with "FYTD"
s2h_1p_fytd <-
  s2h_1p_report_df_3 %>% filter(`Start Date` > "2024-01-20")

s2h_1p_fytd <-
  s2h_1p_fytd %>% mutate("time_period" = "FYTD") %>% select(time_period, everything())


#Union 4 tables back together
s2h_1p_union_1 <- union_all(s2h_1p_lw, s2h_1p_l4w)
s2h_1p_union_2 <- union_all(s2h_1p_union_1, s2h_1p_l52w)
s2h_1p_union_3 <- union_all(s2h_1p_union_2, s2h_1p_fytd)


#Remove unnecessary fields
s2h_1p_union_4 <-
  s2h_1p_union_3 %>% select(-c(walmart_calendar_week, `End Date`, `Start Date`))


#Summarize over time_period, omni fields, vendor_number, banner_description, business_company_description, Channel, Go Forward Disposition
s2h_1p_union_5 <-
  s2h_1p_union_4 %>% group_by(
    time_period,
    omni_category_description,
    omni_category_group_description,
    omni_category_group_number,
    omni_department_description,
    omni_department_number,
    omni_subcategory_description,
    vendor_number,
    banner_description,
    business_company_description,
    Channel,
    `Go Forward Disposition`
  ) %>%
  dplyr::summarise(
    TY_Sales = sum(TY_Sales),
    LY_Sales = sum(LY_Sales),
    TY_Quantity = sum(TY_Quantity),
    LY_Quantity = sum(LY_Quantity)
  )

#Union Final Sales Metrics with Final S2H 1P
final_sales_metrics_3 <- union_all(final_sales_metrics_2, s2h_1p_union_5)

#Output final Sales Metrics file
writexl::write_xlsx(final_sales_metrics_3,
                    path = "//BARRACUDA/SLS_Depts/DEPTS/Mass Channel/WALMART/ED CARDS_Current Year/POS REPORTS/Dashboard/FY25 (2024)/Data/weekly_and_fytd_sales_metrics_csv.xlsx",
                    col_names = TRUE)