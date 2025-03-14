
CREATE VIEW Accident_Location_View AS 
SELECT Acc.Accident_Index, Acc.Date, Loc.Latitude, Loc.Longitude, Loc.Urban_or_Rural_Area 
FROM Accident Acc 
JOIN Location Loc ON Acc.Latitude = Loc.Latitude AND Acc.Longitude = Loc.Longitude; 

 
CREATE VIEW Accident_Details_View AS 
SELECT Acc.Accident_Index, Acc.Date, Acc.Accident_Severity, Acc.Number_of_Casualties, Acc.Number_of_Vehicles, 
       Rd.Road_Type, Rd.Speed_limit, Jun.Junction_Detail 
FROM Accident Acc 
JOIN Road Rd ON Acc.Road_ID = Rd.Road_ID 
JOIN Junction Jun ON Acc.Junction_ID = Jun.Junction_ID; 