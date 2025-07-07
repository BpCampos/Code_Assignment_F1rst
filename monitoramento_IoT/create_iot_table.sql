CREATE TABLE IF NOT EXISTS readings (
    id INT IDENTITY PRIMARY KEY,
    device_id NVARCHAR(50),
    location NVARCHAR(100),
    ip_address NVARCHAR(50),
    temperature FLOAT,
    humidity FLOAT,
    timestamp DATETIME
);
