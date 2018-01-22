/*4. WebLogData(UserName,PaymentGateway, AccessTime) 		GatewaySuccessData(PayMentGateway, SuccessRate).
		john	citibank	19.00						citibank	95
		john	hsbc bank	19.05						hsbc bank	95
		john	sc bank		17.00						sc bank		92
		john	abc bank	17.05						abc bank	85
		rita	sc bank		11.05
		rita	abc bank	11.00

            Identify the user who uses “reliable” payment gateway.  Reliable payment gateway: Payment Gateway  whose transaction success rate is greater than 90%.*/


bagWebLog= LOAD '/home/hduser/Downloads/weblog' USING PigStorage('\t') AS (UserName: CHARARRAY, PaymentGateway: CHARARRAY, AccessTime: FLOAT);

bagGatewaySuccessData= LOAD '/home/hduser/Downloads/gateway' USING PigStorage('\t') AS ( PaymentGateway: CHARARRAY, SuccessRate: DOUBLE);

DESCRIBE bagWebLog;

DESCRIBE bagGatewaySuccessData;

/*
bagWebLog: {UserName: chararray,PaymentGateway: chararray,AccessTime: float}
bagGatewaySuccessData: {PaymentGateway: chararray,SuccessRate: double}   */

DUMP bagWebLog;
/*(john,citibank,19.0)
(john,hsbc bank,19.05)
(john,sc bank,17.0)
(john,abc bank,17.05)
(rita,sc bank,11.05)
(rita,abc bank,11.0)*/

DUMP bagGatewaySuccessData;
/*(citibank,95.0)
(hsbc bank,95.0)
(sc bank,92.0)
(abc bank,85.0)
*/

bagJoined= JOIN bagWebLog BY PaymentGateway, bagGatewaySuccessData BY PaymentGateway;
DESCRIBE bagJoined;
--bagJoined: {bagWebLog::UserName: chararray,bagWebLog::PaymentGateway: chararray,bagWebLog::AccessTime: float,bagGatewaySuccessData::PaymentGateway: chararray,bagGatewaySuccessData::SuccessRate: double}

DUMP bagJoined;
/*(rita,sc bank,11.05,sc bank,92.0)
(john,sc bank,17.0,sc bank,92.0)
(rita,abc bank,11.0,abc bank,85.0)
(john,abc bank,17.05,abc bank,85.0)
(john,citibank,19.0,citibank,95.0)
(john,hsbc bank,19.05,hsbc bank,95.0)*/

bagFinalData= FOREACH bagJoined GENERATE $0,$1,$4;
DESCRIBE bagFinalData;
--bagFinalData: {bagWebLog::UserName: chararray,bagWebLog::PaymentGateway: chararray,bagGatewaySuccessData::SuccessRate: double}

--DUMP bagFinalData;
/*
(rita,sc bank,92.0)
(john,sc bank,92.0)
(rita,abc bank,85.0)
(john,abc bank,85.0)
(john,citibank,95.0)
(john,hsbc bank,95.0)
*/

bagGroupUserName= GROUP bagFinalData BY UserName;
--DUMP bagGroupUserName;
/*
(john,{(john,hsbc bank,95.0),(john,citibank,95.0),(john,abc bank,85.0),(john,sc bank,92.0)})
(rita,{(rita,abc bank,85.0),(rita,sc bank,92.0)})
*/

bagAvgSuccessRate= FOREACH bagGroupUserName GENERATE group, ROUND_TO(AVG(bagFinalData.SuccessRate),2) AS AverageSuccessRate;
DUMP bagAvgSuccessRate;
/*(john,91.67)
(rita,85.0)
*/
bagFilterReliableGateway= FILTER bagAvgSuccessRate BY AverageSuccessRate>90;

DUMP bagFilterReliableGateway;
--(john,91.67)



