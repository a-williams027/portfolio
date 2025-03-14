DELIMITER $$

CREATE PROCEDURE accident_project.DropTables()
BEGIN

    -- Drop Driver table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Driver') THEN
        DROP TABLE accident_project.Driver;
    END IF;

    -- Drop Vehicle table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Vehicle') THEN
        DROP TABLE accident_project.Vehicle;
    END IF;

    -- Drop Vehicle_Type table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Vehicle_Type') THEN
        DROP TABLE accident_project.Vehicle_Type;
    END IF;

    -- Drop Conditions table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Conditions') THEN
        DROP TABLE accident_project.Conditions;
    END IF;

    -- Drop Accident table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Accident') THEN
        DROP TABLE accident_project.Accident;
    END IF;

    -- Drop Junction table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Junction') THEN
        DROP TABLE accident_project.Junction;
    END IF;

    -- Drop Road table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Road') THEN
        DROP TABLE accident_project.Road;
    END IF;

    -- Drop Location table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Location') THEN
        DROP TABLE accident_project.Location;
    END IF;

    -- Drop Authority table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Authority') THEN
        DROP TABLE accident_project.Authority;
    END IF;

    -- Drop Date table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'accident_project' AND table_name = 'Date') THEN
        DROP TABLE accident_project.Date;
    END IF;

END $$

DELIMITER ;
