FaceInPage = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/FaceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCodeCode: chararray, Hobby: chararray);
associates = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/Associates.csv' USING PigStorage(',') AS (Rel_ID: int, PersonA_ID: int, PersonB_ID: int, DateOfRelation: chararray, Description: chararray);
combinedIDsOne = FOREACH associates GENERATE PersonA_ID AS ID;
combinedIDsTwo =FOREACH associates GENERATE PersonB_ID AS ID;
combinedIDs = UNION combinedIDsOne,combinedIDsTwo;
ownerRelationshipCounts = FOREACH (GROUP combinedIDs BY ID) {
    ID = group;
    relationshipCount = COUNT(combinedIDs);
    GENERATE ID as ID:int, relationshipCount as relationshipCount:int;
}
result = JOIN FaceInPage BY ID LEFT OUTER, ownerRelationshipCounts BY ID;
report = FOREACH result GENERATE FaceInPage::Name AS OwnerName, (ownerRelationshipCounts::relationshipCount is not null ? ownerRelationshipCounts::relationshipCount : 0) AS HappinessFactor;
fs -rm -f -r -R C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskD;
STORE report INTO 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskD' USING PigStorage(',');