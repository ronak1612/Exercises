
bagNov= LOAD '/home/hduser/Downloads/D11' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagDec= LOAD '/home/hduser/Downloads/D12' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagJan= LOAD '/home/hduser/Downloads/D01' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);

bagFeb= LOAD '/home/hduser/Downloads/D02' USING PigStorage(';') AS (DateTimes: CHARARRAY, CustomerID: INT, Age: CHARARRAY, ResidenceArea: CHARARRAY,ProductCategory: INT ,ProductID: CHARARRAY,Qty: INT,TotalCost: DOUBLE,TotalSales: DOUBLE);


--(A2)Find total gross profit made by each product and also by each category for all the 4 months data.


allmonthData= UNION bagNov, bagDec, bagJan, bagFeb;

requiredData= FOREACH allmonthData GENERATE $5, $7, $8;

DESCRIBE requiredData;
--requiredData: {ProductID: chararray,TotalCost: double,TotalSales: double}


groupingProductID= GROUP requiredData BY ProductID;

DESCRIBE groupingProductID;
--groupingProductID: {group: chararray,requiredData: {(ProductID: chararray,TotalCost: double,TotalSales: double)}}

--DUMP groupingProductID;

sumCostSaleData= FOREACH groupingProductID GENERATE group, SUM(requiredData.TotalCost) AS SUMCOST, SUM(requiredData.TotalSales) AS SUMSALES;

DESCRIBE sumCostSaleData;
--grossProfitData: {group: chararray,SUMCOST: double,SUMSALES: double}

grossProfit= FOREACH sumCostSaleData GENERATE group,  ((SUMSALES-SUMCOST)) AS GROSSPROFIT;

DESCRIBE grossProfit;
--grossProfit: {group: chararray,GROSSPROFIT: double}

--DUMP grossProfit;

orderGrossProfit= ORDER grossProfit BY GROSSPROFIT DESC;

top10grossProfit= LIMIT orderGrossProfit 10;

--DUMP top10grossProfit;


/* (20562687     ,37.333333333333336)
(4714298810208,2.7933333333333334)
(4714298808236,1.9772727272727273)
(4711821100584,1.6090909090909091)
(20454388     ,1.425)
(4717362800648,1.4172099087353325)
(20454395     ,1.4)
(20569150     ,1.368421052631579)
(20564520     ,1.25)
(20553111     ,1.2403846153846154)*/




requiredDataCategory= FOREACH allmonthData GENERATE  $4, $7, $8;

DESCRIBE requiredDataCategory;
--requiredDataCategory: {ProductCategory: int,TotalCost: double,TotalSales: double}

groupingProductCategory= GROUP requiredDataCategory BY ProductCategory;

DESCRIBE groupingProductCategory;
--groupingProductCategory: {group: int,requiredDataCategory: {(ProductCategory: int,TotalCost: double,TotalSales: double)}}

sumCostSaleDataCategory= FOREACH groupingProductCategory GENERATE group, SUM(requiredDataCategory.TotalCost) AS SUMCOST, 
SUM(requiredDataCategory.TotalSales) AS SUMSALES;

DESCRIBE sumCostSaleDataCategory;
--sumCostSaleDataCategory: {group: int,SUMCOST: double,SUMSALES: double}

--DUMP sumCostSaleDataCategory;
/*(100101,132815.0,156913.0)
(100102,698807.0,831750.0)
(100103,118129.0,148481.0)
(100104,57115.0,73549.0)
(100105,17783.0,22609.0)
(100106,202327.0,244784.0)
(100107,41376.0,49733.0)
*/


grossProfitCategory= FOREACH sumCostSaleDataCategory GENERATE group,  ((SUMSALES-SUMCOST)) AS GROSSPROFIT;

--DESCRIBE grossProfitCategory;
--grossProfitCategory: {group: int,GROSSPROFIT: double

orderGrossProfitCategory= ORDER grossProfitCategory BY GROSSPROFIT DESC;


finalGrossProfitCategory= LIMIT orderGrossProfitCategory 10;

DUMP finalGrossProfitCategory; 

/*(760112,1.16622691292876)
(590106,0.9477611940298507)
(751207,0.9319877139096094)
(590105,0.8765957446808511)
(520423,0.8442708333333333)
(590202,0.811088295687885)
(760638,0.7505285412262156)
(760750,0.7464406779661017)
(300905,0.6993327773144287)
(500108,0.6889030072003388)*/



