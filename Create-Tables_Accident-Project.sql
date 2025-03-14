DELIMITER $$

CREATE PROCEDURE accident_project.CreateTables()
BEGIN
    -- Create Date table 
    CREATE TABLE IF NOT EXISTS accident_project.Date (
        Date DATE PRIMARY KEY NOT NULL,
        Day_of_Week VARCHAR(9),
        Year VARCHAR(4)
    );

    -- Create Authority table 
    CREATE TABLE IF NOT EXISTS accident_project.Authority (
        District_ID INT PRIMARY KEY,
        Local_Authority_District VARCHAR(255),
        Local_Authority_Highway TEXT,
        Police_Force TEXT,
        InScotland VARCHAR(3)
    );

    -- Create Location table 
    CREATE TABLE IF NOT EXISTS accident_project.Location (
        Latitude DECIMAL(8,6),
        Longitude DECIMAL(9,6),
        District_ID INT,
        Location_Northing_OSGR INT,
        Location_Easting_OSGR INT,
        LSOA_of_Accident_Location VARCHAR(10),
        Urban_or_Rural_Area VARCHAR(15),
        PRIMARY KEY (Latitude, Longitude)
    );

    -- Create Road table 
    CREATE TABLE IF NOT EXISTS accident_project.Road (
        Road_ID INT PRIMARY KEY,
        First_Road_Number INT,
        Second_Road_Number INT,
        First_Road_Class VARCHAR(15),
        Second_Road_Class VARCHAR(15),
        Road_Type VARCHAR(255),
        Speed_limit INT
    );

    -- Create Junction table 
    CREATE TABLE IF NOT EXISTS accident_project.Junction (
        Junction_ID INT PRIMARY KEY,
        Junction_Control TEXT,
        Junction_Detail TEXT
    );

    -- Create Accident table 
    CREATE TABLE IF NOT EXISTS accident_project.Accident (
        Accident_Index VARCHAR(15) PRIMARY KEY,
        Date DATE,
        Latitude DECIMAL(8,6),
        Longitude DECIMAL(9,6),
        Road_ID INT,
        Junction_ID INT,
        Time TIME,
        Accident_Severity VARCHAR(10),
        Number_of_Casualties INT,
        Number_of_Vehicles INT,
        Carriageway_Hazards TEXT,
        Pedestrian_Crossing_Human_Control INT,
        Pedestrian_Crossing_Physical_Facilities INT,
        Did_Police_Officer_Attend_Scene_of_Accident INT
    );

    -- Create Conditions table 
    CREATE TABLE IF NOT EXISTS accident_project.Conditions (
        Date DATE,
        Latitude VARCHAR(20),
        Longitude VARCHAR(20),
        Weather_Conditions TEXT,
        Road_Surface_Conditions TEXT,
        Light_Conditions TEXT,
        Special_Conditions_at_Site TEXT,
        PRIMARY KEY (Date, Latitude, Longitude)
    );

    -- Create Vehicle Type table  
    CREATE TABLE IF NOT EXISTS accident_project.Vehicle_Type (
        Vehicle_ID INT PRIMARY KEY,
        Vehicle_Type VARCHAR(255),
        Towing_and_Articulation TEXT,
        Engine_Capacity_CC INT,
        Propulsion_Code TEXT,
        Make VARCHAR(255),
        Model VARCHAR(255)
    );

    -- Create Vehicle table 
    CREATE TABLE IF NOT EXISTS accident_project.Vehicle (
        Accident_Index VARCHAR(15),
        Vehicle_Reference INT,
        Vehicle_ID INT,
        Vehicle_Manoeuvre TEXT,
        Vehicle_Location_Restricted_Lane INT,
        Junction_Location TEXT,
        Skidding_and_Overturning TEXT,
        Hit_Object_in_Carriageway TEXT,
        Vehicle_Leaving_Carriageway TEXT,
        Hit_Object_off_Carriageway TEXT,
        X1st_Point_of_Impact TEXT,
        Was_Vehicle_Left_Hand_Drive VARCHAR(3),
        Age_of_Vehicle INT,
        PRIMARY KEY (Accident_Index, Vehicle_Reference)
    );

    -- Create Driver table 
    CREATE TABLE IF NOT EXISTS accident_project.Driver (
        Accident_Index VARCHAR(15),
        Vehicle_Reference INT,
        Journey_Purpose_of_Driver TEXT,
        Sex_of_Driver VARCHAR(10),
        Age_Band_of_Driver TEXT,
        Driver_IMD_Decile INT,
        Driver_Home_Area_Type TEXT,
        PRIMARY KEY (Accident_Index, Vehicle_Reference)
    );
END $$

DELIMITER ;


 