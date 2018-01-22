
bagNov= LOAD '/home/hduser/Downloads/D11' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagDec= LOAD '/home/hduser/Downloads/D12' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagJan= LOAD '/home/hduser/Downloads/D01' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagFeb= LOAD '/home/hduser/Downloads/D02' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

allmonthData= UNION bagNov, bagDec, bagJan, bagFeb;
requiredData= FOREACH allmonthData GENERATE $0, $5, $8;

monthgeneration= FOREACH requiredData GENERATE $1, $2, SUBSTRING($0,5,7) AS Month;

DESCRIBE monthgeneration;

--monthgeneration: {ProductID: chararray,TotalSales: double,Month: chararray}
 
groupingProductID= GROUP monthgeneration BY (Month,ProductID) ;


sumCostSalesData= FOREACH groupingProductID GENERATE group,SUM(monthgeneration.TotalSales) AS SUMSALES;

prints= LIMIT sumCostSalesData 100;

DUMP prints;
--orderGrossProfit= ORDER sumCostSaleData BY SUMSALES desc ;

--top10SalesProfit= LIMIT orderGrossProfit 16;



--DUMP top10SalesProfit;

