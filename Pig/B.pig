
--(B)Find out the top 4 or top 10 product being sold in the monthly basis and in all the 4 months..
-- Criteria for top should be sales amount.

bagNov= LOAD '/home/hduser/Downloads/D11' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagDec= LOAD '/home/hduser/Downloads/D12' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagJan= LOAD '/home/hduser/Downloads/D01' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagFeb= LOAD '/home/hduser/Downloads/D02' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);



allmonthData= UNION bagNov, bagDec, bagJan, bagFeb;
requiredData= FOREACH allmonthData GENERATE $5, $7, $8;

groupingProductID= GROUP requiredData BY ProductID;

SumCostSaleData= FOREACH groupingProductID GENERATE group,SUM(requiredData.TotalSales) AS SUMSALES;

orderGrossProfit= ORDER SumCostSaleData BY SUMSALES DESC;

top10SalesProfit= LIMIT orderGrossProfit 10;

--DUMP top10SalesProfit;

/* (8712045008539,1540503.0)
(4710628131012,675112.0)
(4710114128038,514601.0)
(4711588210441,491292.0)
(20553418     ,470501.0)
(4710628119010,433380.0)
(4909978112950,432596.0)
(8712045000151,428530.0)
(7610053910787,392581.0)
(4719090900065,385626.0)
*/




