## 20200922_GoogleMapApi及計算經緯度距離
## 程式碼撰寫: 中山財管 蘇彥庭 研究助理
## 參考資料: https://medium.com/front-end-augustus-study-notes/google-map-api-1-a4e794b0162f
##           https://medium.com/@icelandcheng/使用google-map-api-geocoding-api-得到點位縣市鄉鎮資料-25bf5f0e4a2

# 前置作業說明:
# 首先需先至Google Map申請帳戶開啟API金鑰
# https://cloud.google.com/maps-platform/
# Google Map目前需要收費 但有提供免費額度(每月200美元)
# 申請帳戶需要輸入個人信用卡資料
# API服務部分記得要啟用Geocoding API才能執行以下程式碼

# 載入套件
library(tidyverse)
library(jsonlite)
library(geosphere)

# API金鑰
apiKey <- "~~~~~~~~~~~~~~~~~~~~~~~~~~~~"   # 執行前請填入API!!!

# 查詢目標地址
targetAddress <- c("高雄", "台北", "紐約", "倫敦", "柏林", "東京", "北京", "坎培拉")

# 建立儲存表
queryResultData <- NULL  # 查詢結果繪製表
unsuccesssQuery <- NULL  # 查詢失敗目標

# 執行迴圈取得資料
ix <- 1
for(ix in c(1:length(targetAddress))){
  
  cat(paste0("目前正在查詢目標地址: ", targetAddress[ix], " 進度: ", ix, " / ", length(targetAddress), "\n"))
  
  # Query網址
  queryUrl <- paste0("https://maps.googleapis.com/maps/api/geocode/json?address=", targetAddress[ix], "&key=", apiKey)
  
  # 下載資料
  data <- fromJSON(queryUrl)
  
  # 判斷是否有下載成功
  if(data$status == "OK"){
    
    # 紀錄查詢結果
    result <- tibble(targetAddress = targetAddress[ix],                             # 查詢目標地址
                     lng = data[["results"]][["geometry"]][["location"]][["lng"]],  # 經度
                     lat = data[["results"]][["geometry"]][["location"]][["lat"]]   # 緯度
                     )  
    
    # 紀錄資訊
    queryResultData <- queryResultData %>% bind_rows(result)
    
  }else{
   
    warning(paste0("查詢目標地址: '", targetAddress[ix], "' 有問題 請手動確認!\n")) 
    unsuccesssQuery <- c(unsuccesssQuery, targetAddress[ix])
  }
}

# 計算兩點之間的距離
# 此處計算方式與兩個專有名詞有關係
# 大圓距離(Great-circle distance) (wiki: https://en.wikipedia.org/wiki/Great-circle_distance)
# 參考橢球體(Reference ellipsoid) (wiki: https://en.wikipedia.org/wiki/Reference_ellipsoid)
# 最常參考的橢球體為WGS84
# 此處採用geosphere套件的distVincentyEllipsoid函數計算距離(橢球體設定默認為WGS84)

# 計算距離矩陣
distanceMatrix <- distm(queryResultData[, c("lng", "lat")], 
                        queryResultData[, c("lng", "lat")], 
                        fun = distVincentyEllipsoid)
colnames(distanceMatrix) <- rownames(distanceMatrix) <- queryResultData$targetAddress
distanceMatrix
