FaceInPage = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/FaceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
accessLogs = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/AccessLogs.csv' USING PigStorage(',') AS (Acc_ID: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);
accessCounts = foreach (GROUP accessLogs BY WhatPage) {
    pageID = group;
    accessCount = COUNT(accessLogs);
    GENERATE pageID AS ID, accessCount AS count;
}
joinedData = JOIN FaceInPage BY ID, accessCounts BY ID;
sortedData = ORDER joinedData BY accessCounts::count DESC;
top10Pages = LIMIT sortedData 10;
report = FOREACH top10Pages GENERATE FaceInPage::ID AS ID, FaceInPage::Name AS Name, FaceInPage::Nationality AS Nationality;
fs -rm -f -r -R C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskB;
STORE report INTO 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskB' USING PigStorage(',');