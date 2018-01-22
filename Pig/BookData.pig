
bagBook= LOAD '' USING PigStorage(',') AS ( BookID:INT, Price:INT, AuthorID:INT);

bagAuthor= LOAD '' USING PigStorage(',') AS ( AuthorID: INT, AuthorName STRING);


bagJoin= JOIN bagBook by AuthorID, bagAuthor by AuthorID;

bagFinal= FOREACH bagJoin GENERATE $0, $1, $2, $5;

priceFilter= FILTER bagFinal by Price>200;

NameFilter= Filter priceFilter by INDEXOF(AuthorName,'J',0)==0;

------------------------------------------------------------------------------------------

bagMedical= LOAD '' USING PigStorage() AS (Name: STRING, Department: STRING, ClaimAmt: INT);

byName= GROUP bagMedical BY Name;


groupAvg= FOREACH byName GENERATE  group AS Name, ROUND_TO(AVG(ClaimAmt),2) AS Average;

------------------------------------------------------------------------------------------

webLog= LOAD '' USING PigStorage('\t') AS (User: STRING, Gateway: STRING, AccesTime: DOUBLE);

gatewayLog= LOAD '' USING PigStorage('\t') AS ( Gateway: STRING, SuccessRate: DOUBLE);


joinData= JOIN weblog BY Gateway, gatewayLog BY Gateway;

newBag=FOREACH joinData GENERATE $0, $1, $4;

groupBag= GROUP newBag BY $0;

avgRate= FOREACH groupBag GENERATE group, AVD(groupBag.SuccessRate) AS AvgSuccess;

RESULT= FILTER avgRate by AvgSuccess>90.0F;



















