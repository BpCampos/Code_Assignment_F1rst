IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'readings' AND TABLE_SCHEMA = 'dbo'
)
BEGIN

CREATE TABLE IF NOT EXISTS readings (
    id INT IDENTITY PRIMARY KEY,
    device_id NVARCHAR(50),
    location NVARCHAR(100),
    ip_address NVARCHAR(50),
    temperature FLOAT,
    humidity FLOAT,
    timestamp DATETIME
);

END