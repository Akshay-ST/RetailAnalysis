CREATE TABLE Calls (
    Caller VARCHAR(50),
    Receiver VARCHAR(50),
    Date DATE,
    Duration INT
);

INSERT INTO Calls (Caller, Receiver, Date, Duration) VALUES
('A', 'B', '2024-01-01', 58),
('A', 'C', '2024-01-01', 34),
('C', 'B', '2024-01-02', 42),
('C', 'A', '2024-01-02', 120),
('B', 'C', '2024-01-01', 134),
('B', 'A', '2024-01-02', 202);

select * from Calls;


SELECT
    LEAST(Caller, Receiver) AS Caller1,
    GREATEST(Caller, Receiver) AS Caller2,
    SUM(Duration) AS Total_Duration
FROM Calls
GROUP BY 1,2
ORDER BY 1;

