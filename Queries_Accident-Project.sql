SELECT Au.District_ID, Au.Local_Authority_District, COUNT(*) AS Num_Accidents
FROM Accident Acc
JOIN Location L ON Acc.Latitude = L.Latitude AND Acc.Longitude = L.Longitude
JOIN Authority Au ON Au.District_ID = L.District_ID
GROUP BY Au.District_ID, Au.Local_Authority_District
ORDER BY COUNT(*) DESC
LIMIT 5;

SELECT d.Year, SUM(Number_of_Casualties) AS Total_Casualties
FROM Accident Acc
JOIN Date d ON Acc.Date = d.Date
GROUP BY Year
ORDER BY Year;

SELECT SUM(Number_of_Casualties) AS Total_Casualties, vt.Vehicle_Type
FROM Accident Acc
JOIN Vehicle V ON Acc.Accident_Index = V.Accident_Index
JOIN Vehicle_Type vt ON V.Vehicle_ID = vt.Vehicle_ID
GROUP BY vt.Vehicle_Type
ORDER BY SUM(Number_of_Casualties) DESC
LIMIT 5;

SELECT COUNT(*) AS Num_Accidents, Road_Type, AVG(Speed_limit) AS Avg_Speed_Limit
FROM Accident_Details_View
GROUP BY Road_Type
ORDER BY COUNT(*)  DESC;

SELECT COUNT(*) AS Num_Accidents, AVG(Number_of_Vehicles) AS Avg_Num_Vehicles
FROM Accident_Details_View
WHERE Junction_Detail LIKE '%Crossroad%'