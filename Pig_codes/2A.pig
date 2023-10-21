FaceInPage = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/FaceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
filteredUsers = FILTER FaceInPage BY Nationality == 'United States of America';
report = FOREACH filteredUsers GENERATE Name, Hobby;
fs -rm -f -r -R C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskA;
STORE report INTO 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskA' USING PigStorage(',');