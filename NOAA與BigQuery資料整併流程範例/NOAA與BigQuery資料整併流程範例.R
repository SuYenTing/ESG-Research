## 20201007_NOAA_BigQuery資料整併流程範例
## 指導教授: 中山財管 王昭文 教授
## 程式碼撰寫: 中山財管 蘇彥庭 研究助理
rm(list=ls());gc()
library(rvest)
library(jsonlite)
library(geosphere)
library(tidyverse)


########## 自製函數 ##########
# 字首改為大寫(使bigQueryData的州別和wiki的州別全名能夠一致)
CapitalString <- function(y) {
  c <- strsplit(y, " ")[[1]]
  paste(toupper(substring(c, 1, 1)), substring(c, 2), sep = "", collapse =" ")
}

# NOAA Event資料整理所需函數(分割event欄位的事件名稱及發生期間)
NOAAEventSplit <- function(x, site){
  y <- trimws(x)    # 刪除字串前後空白
  y <- y[y != ""]   # 剔除空白元素
  return(y[site])
}

########## 下載NOAA Billion-Dollar Weather and Climate Disasters: Events頁面資料 ##########
# 目標網址
dataStartYear <- 2000
dataeEndYear <- 2020
targetUrl <- paste0("https://www.ncdc.noaa.gov/billions/events/US/", dataStartYear, "-", dataeEndYear)

# 下載資料
eventData <- read_html(targetUrl) %>%
  html_nodes(css = "td") %>%
  html_text() %>%
  matrix(ncol = 6, byrow = T)

# 整理資料
colnames(eventData) <- c("event", "beginDate", "endDate", "summary", "cpiAdjCost", "deaths")
eventData <- eventData %>%
  as_tibble() %>%
  mutate(eventName = sapply(str_split(event, "\\n"), NOAAEventSplit, site = 1),
         eventYearMonth = sapply(str_split(event, "\\n"), NOAAEventSplit, site = 2),
         eventName = gsub("†", "", eventName)) %>%
  select(eventName, eventYearMonth, beginDate, endDate, summary)

# 刪除重複事件
eventData <- eventData[!duplicated(eventData$eventName), ]

# 儲存資料
save(eventData, file = "eventData.Rdata")


########## 下載美國州別全名與對應資料 ##########
# 目標網址
targetUrl <- "https://en.wikipedia.org/wiki/List_of_U.S._state_and_territory_abbreviations"

# 下載資料
stateInfoData <- read_html(targetUrl) %>%
  html_nodes(xpath = "//*[@id='mw-content-text']/div[1]/table[1]/tbody/tr/td") %>%
  html_text() %>%
  matrix(ncol = 10, byrow = T) %>%
  .[, c(1, 4)]

# 整理資料
colnames(stateInfoData) <- c("fullName", "name")
stateInfoData <- stateInfoData %>%
  as_tibble() %>%
  mutate(fullName = gsub("^\\s+", "", fullName),          # 刪除文字前面空白
         fullName = sapply(fullName, CapitalString),      # 字首大寫
         name = gsub("^\\s+", "", name)) %>%              # 刪除文字前面空白
  filter(name != "")                                      # 清除無簡寫的州別

# 儲存資料
save(stateInfoData, file = "stateInfoData.Rdata")


########## 讀入BigQuery資料 ##########
# 資料表位置: bigquery-public-data:noaa_historic_severe_storms
bigQueryData <- read.csv("storms_2020.csv", stringsAsFactors = F) %>% as_tibble()

# 整理資料
bigQueryData <- bigQueryData %>%
  mutate(state = sapply(state, CapitalString),                        # 字首大寫
         event_begin_date = as.character(as.Date(event_begin_time)),  # 建立事件發生日期欄位
         event_end_date = as.character(as.Date(event_end_time)))      # 建立事件結束日期欄位

# 儲存資料
save(bigQueryData, file = "bigQueryData.Rdata")


########## 讀取資料 ##########
load("eventData.Rdata")
load("stateInfoData.Rdata")
load("bigQueryData.Rdata")


########## 整理事件對應鍵值 ##########
# 篩選事件範圍
eventData <- eventData %>% filter(beginDate >= "2020-01-01")

# 目標事件關鍵字
targetEventType <- c("flood", "flash flood", "wildfire", "tropical storm")

# 建立空儲存表
eventDetailData <- NULL

ix <- 1
for(ix in c(1:nrow(eventData))){
  
  # 比對事件敘述是否有提及州別
  eventState <- c(stateInfoData$fullName[sapply(stateInfoData$fullName, grepl, eventData$summary[ix])],
                  stateInfoData$fullName[sapply(stateInfoData$name, grepl, eventData$summary[ix])])
  
  # 比對事件敘述是否有提及目標事件
  eventType <- targetEventType[sapply(targetEventType, grepl, eventData$summary[ix])]
  
  # 事件起始與結束日
  eventBeginDate <- eventData$beginDate[ix]
  eventEndDate <- eventData$endDate[ix]
  
  # 搜尋BigQuery對應期間 州別及事件資訊
  iEventDetailData <- bigQueryData %>%
    filter(event_begin_date >= eventBeginDate, event_end_date <= eventEndDate) %>%
    filter(state %in% eventState) %>%
    filter(event_type %in% eventType) %>%
    mutate(eventName = eventData$eventName[ix],
           beginDate = eventData$beginDate[ix],
           endDate = eventData$endDate[ix]) %>%
    select(eventName, beginDate, endDate, state, event_type, cz_fips_code, cz_name, 
           event_begin_time, event_end_time, event_begin_date, event_end_date, 
           injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, 
           damage_property, damage_crops, source, event_latitude, event_longitude)
  
  # 紀錄資料
  eventDetailData <- eventDetailData %>% bind_rows(iEventDetailData)
}


########## 查詢公司總部地址對應經緯度 ##########
# 自建公司地址清單
companyInfoData <- tibble(name = c("AAPL", "TESLA"),
                          address = c("One Apple Park Way , Cupertino, CA 95014-2083",
                                      "3500 Deer Creek Road Palo Alto, CA 94304"))

# API金鑰
apiKey <- "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"   # 執行前請填入API!!!

# 建立儲存表
queryResultData <- NULL  # 查詢結果繪製表
unsuccesssQuery <- NULL  # 查詢失敗目標

# 執行迴圈取得資料
ix <- 1
for(ix in c(1:nrow(companyInfoData))){
  
  cat(paste0("目前正在查詢目標地址: ", companyInfoData$name[ix], " 進度: ", ix, " / ", nrow(companyInfoData), "\n"))
  
  # Query網址
  queryUrl <- paste0("https://maps.googleapis.com/maps/api/geocode/json?address=", 
                     companyInfoData$address[ix], "&key=", apiKey)
  
  # 下載資料
  data <- fromJSON(URLencode(queryUrl))
  
  # 判斷是否有下載成功
  if(data$status == "OK"){
    
    # 紀錄查詢結果
    result <- tibble(name = companyInfoData$name[ix],                               # 查詢公司名稱
                     address = companyInfoData$address[ix],                         # 查詢目標地址
                     lng = data[["results"]][["geometry"]][["location"]][["lng"]],  # 經度
                     lat = data[["results"]][["geometry"]][["location"]][["lat"]]   # 緯度
    )  
    
    # 紀錄資訊
    queryResultData <- queryResultData %>% bind_rows(result)
    
  }else{
    
    warning(paste0("查詢目標地址: '", companyInfoData$address[ix], "' 有問題 請手動確認!\n")) 
    unsuccesssQuery <- c(unsuccesssQuery, companyInfoData$address[ix])
  }
}


########## 計算公司與事件發生地之間的經緯度 ##########
# 刪除經緯度缺值資料
eventDetailData <- eventDetailData %>%
  filter(!is.na(event_longitude), !is.na(event_latitude))

# 建立距離儲存表
distanceData <- NULL
ix <- iy <- 1
for(ix in c(1:nrow(queryResultData))){
  for(iy in c(1:nrow(eventDetailData))){

    # 計算距離
    distance <- distm(queryResultData[ix, c("lng", "lat")], 
                      eventDetailData[iy, c("event_longitude", "event_latitude")], 
                      fun = distVincentyEllipsoid) %>%
      as.vector()
    
    # 除錯機制: 若距離為NA 則停止程式
    if (is.na(distance)) stop()
    
    # 紀錄資料
    distanceData <- distanceData %>%
      bind_rows(tibble(name = queryResultData$name[ix],
                       eventName = eventDetailData$eventName[iy],
                       event_cz = eventDetailData$cz_name[iy],
                       event_begin_time = eventDetailData$event_begin_time[iy],
                       distance = distance))
  }
}

