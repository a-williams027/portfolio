DELIMITER $$

CREATE PROCEDURE accident_project.AlterTables()
BEGIN

    -- Add foreign key constraints

    ALTER TABLE accident_project.Location 
    ADD CONSTRAINT FK_Location_Authority 
    FOREIGN KEY (District_ID) REFERENCES accident_project.Authority(District_ID);

    ALTER TABLE accident_project.Accident 
    ADD CONSTRAINT FK_Accident_Location 
    FOREIGN KEY (Latitude, Longitude) REFERENCES accident_project.Location(Latitude, Longitude);

    ALTER TABLE accident_project.Accident 
    ADD CONSTRAINT FK_Accident_Road 
    FOREIGN KEY (Road_ID) REFERENCES accident_project.Road(Road_ID);

    ALTER TABLE accident_project.Accident 
    ADD CONSTRAINT FK_Accident_Junction 
    FOREIGN KEY (Junction_ID) REFERENCES accident_project.Junction(Junction_ID);

    ALTER TABLE accident_project.Conditions 
    ADD CONSTRAINT FK_Condition_Date 
    FOREIGN KEY (Date) REFERENCES accident_project.Date(Date);

    ALTER TABLE accident_project.Vehicle 
    ADD CONSTRAINT FK_Vehicle_Accident 
    FOREIGN KEY (Accident_Index) REFERENCES accident_project.Accident(Accident_Index);

    ALTER TABLE accident_project.Vehicle 
    ADD CONSTRAINT FK_Vehicle_VehicleType 
    FOREIGN KEY (Vehicle_ID) REFERENCES accident_project.Vehicle_Type(Vehicle_ID);

    ALTER TABLE accident_project.Driver 
    ADD CONSTRAINT FK_Driver_Vehicle 
    FOREIGN KEY (Accident_Index, Vehicle_Reference) 
    REFERENCES accident_project.Vehicle(Accident_Index, Vehicle_Reference);

END $$

DELIMITER ;
