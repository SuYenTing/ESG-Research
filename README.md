# ESG計畫相關程式碼

## GoogleMapAPI及計算經緯度距離實作

此程式主要是透過Google Map API找出兩個地點的經緯度並計算直線距離，內容包含：

* 如何申請Google Map API
* R軟體與Google Map API串接程式碼
* 利用兩地點的經緯度計算距離

詳細說明可參考簡報內容。

## NOAA與BigQuery資料整併流程範例

NOAA為美國國家海洋暨大氣總署英文簡稱，該署有提供美國全境的災害資料。

NOAA有提供一個歷年以來損失超過十億美元的重大災害[資訊頁面](https://www.ncdc.noaa.gov/billions/events/US/2020)，但該頁面資訊僅列出影響的洲別(States)，並沒有列出郡(County)資訊。

而在Google的BigQuery中，有NOAA紀錄災害通報的詳細資訊，其資料有列出洲別(States)及郡(County)資訊，但並沒有提供那些災害事件是屬於超過十億美元重大災害計算範圍內。

因此程式的目標主要在整合NOAA及Google的BigQuery資訊，找出每個十億美元以上災害影響到美國那些郡(County)。

由於十億美元以上災害的描述中對於影響洲別的用詞有些是用簡寫，有些則是用全稱，因此先透過維基百科建立美國洲別簡寫與全稱的字典，透過字串比對找出十億美元以上災害影響的洲別。接下來在BigQuery資料，以十億美元以上災害影響的洲別及發生期間，篩選出對應洲別與期間的災害通報，藉此即可得到十億美元以上災害影響到的郡(County)。


