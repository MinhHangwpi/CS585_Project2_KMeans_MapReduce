FaceInPage = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/FaceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
accessLogs = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/AccessLogs.csv' USING PigStorage(',') AS (Acc_ID: int, ByWho: int, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);
accessCounts = FOREACH (GROUP accessLogs BY ByWho) {
    ByWho = group;
    totalAccesses = COUNT(accessLogs);
    distinctPagesAccessed = DISTINCT accessLogs.WhatPage;
    totalDistinctPagesAccessed = COUNT(distinctPagesAccessed);
    GENERATE ByWho AS ID:int, totalAccesses AS TotalAccesses:int, totalDistinctPagesAccessed AS DistinctPagesAccessed:int;
}
result = JOIN FaceInPage BY ID LEFT OUTER, accessCounts BY ID;
report = FOREACH result GENERATE FaceInPage::Name AS OwnerName, (accessCounts::TotalAccesses is not null ? accessCounts::TotalAccesses : 0) AS TotalAccesses, (accessCounts::DistinctPagesAccessed is not null ? accessCounts::DistinctPagesAccessed : 0) AS DistinctPagesAccessed;
fs -rm -f -r -R C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskE;
STORE report INTO 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskE' USING PigStorage(',');