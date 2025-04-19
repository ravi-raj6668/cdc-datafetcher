EXEC sys.sp_cdc_enable_db;


select * from sys.databases;


use data_event;

CREATE DATABASE data_event;



CREATE TABLE dbo.consumer_event (
                                    id INT IDENTITY(1,1) PRIMARY KEY,
                                    event_name NVARCHAR(100),
                                    event_description NVARCHAR(255),
                                    event_timestamp DATETIME DEFAULT GETDATE()
);


select * from consumer_event order by id desc;

select count(*) from consumer_event;

delete from consumer_event ;

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'consumer_event',
    @role_name = NULL,
    @supports_net_changes = 0;


SELECT * from consumer_event;



EXEC xp_servicecontrol 'START', 'SQLServerAgent';



SELECT servicename, startup_type_desc, status_desc
FROM sys.dm_server_services
WHERE servicename LIKE '%Agent%';
--GO




INSERT INTO dbo.consumer_event (event_name, event_description, event_timestamp)
VALUES ('UserLogin', 'User logged in from mobile app', '2025-04-18T23:30:00');




















SELECT servicename, startup_type_desc, status_desc
FROM sys.dm_servers_services
WHERE servicename LIKE '%Agent%';




EXEC sys.sp_cdc_enable_db;



EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'consumer_event',
    @role_name     = NULL,
    @supports_net_changes = 0;


