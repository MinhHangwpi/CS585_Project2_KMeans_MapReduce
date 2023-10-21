FaceInPage = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/FaceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
report = FOREACH (GROUP FaceInPage BY Nationality) {
    Nationality = group;
    citizenCount = COUNT(FaceInPage);
    GENERATE Nationality as Nationality:chararray, citizenCount as citizenCount:int;
}
fs -rm -f -r -R C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskC;
STORE report INTO 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskC' USING PigStorage(',');