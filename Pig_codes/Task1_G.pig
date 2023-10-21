FaceInPage = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/FaceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: chararray, Hobby: chararray);
accessLogs = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/AccessLogs.csv' USING PigStorage(',') AS (Acc_ID: int, ByWho: chararray, WhatPage: int, TypeOfAccess: chararray, AccessTime: chararray);
latest_accessed = FOREACH (GROUP accessLogs BY WhatPage) {
    WhatPage = group;
    latest_accessed_date = MAX(accessLogs.AccessTime);
    GENERATE WhatPage as ID:int, latest_accessed_date as latest_date:int;
}
filtered_access = FILTER latest_accessed BY latest_date >= 870400;
result = JOIN filtered_access by ID, FaceInPage by ID;
report = FOREACH result GENERATE FaceInPage::ID AS ID, FaceInPage::Name AS Name;
fs -rm -f -r -R C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskG;
STORE report INTO 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskG' USING PigStorage(',');