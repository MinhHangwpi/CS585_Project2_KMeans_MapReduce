FaceInPage = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/FaceInPage.csv' USING PigStorage(',') AS (ID: int, Name: chararray, Nationality: chararray, CountryCode: int, Hobby: chararray);
associates = LOAD 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Input/full_scale_files/Associates.csv' USING PigStorage(',') AS (Rel_ID: int, PersonA_ID: int, PersonB_ID: int, DateOfRelation: chararray, Description: chararray);
combinedIDsOne = FOREACH associates GENERATE PersonA_ID AS OwnerID;
combinedIDsTwo =FOREACH associates GENERATE PersonB_ID AS OwnerID;
combinedIDs = UNION combinedIDsOne,combinedIDsTwo;
ownerRelationshipCounts = FOREACH (GROUP combinedIDs BY OwnerID) {
    ownerID = group;
    relationshipCount = COUNT(combinedIDs);
    GENERATE ownerID as ownerID:int, relationshipCount as relationshipCount:int;
}
relationshipCounts = FOREACH (GROUP ownerRelationshipCounts ALL) GENERATE SUM(ownerRelationshipCounts.relationshipCount) AS totalRelationships:long, COUNT(ownerRelationshipCounts) AS numOwners:long, ((double)SUM(ownerRelationshipCounts.relationshipCount) / (double)COUNT(ownerRelationshipCounts)) AS averageRelationships;
popularOwners = FILTER ownerRelationshipCounts BY relationshipCount > (long)relationshipCounts.averageRelationships;
result = JOIN popularOwners BY ownerID, FaceInPage BY ID;
report = FOREACH result GENERATE FaceInPage::ID AS ID, FaceInPage::Name AS Name, popularOwners::relationshipCount AS RelationshipCount;
fs -rm -f -r -R C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskH;
STORE report INTO 'C:/CourseWork/BigDataManagement/Projects/Assignment2/Output/TaskH' USING PigStorage(',');