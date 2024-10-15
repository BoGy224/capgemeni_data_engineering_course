SELECT * from dbo.TranformedData 
WHERE price IS NULL 
   OR price <= 0
   OR minimum_nights IS NULL
   OR minimum_nights <= 0
   OR availability_365 IS NULL
   OR availability_365 < 0;

SELECT *
FROM dbo.TranformedData
WHERE latitude IS NULL
   OR longitude IS NULL
   OR latitude < -90 OR latitude > 90
   OR longitude < -180 OR longitude > 180;

SELECT DISTINCT room_type
FROM dbo.TranformedData
WHERE room_type NOT IN ('Entire home/apt', 'Private room', 'Shared room');

SELECT id, COUNT(*)
FROM dbo.TranformedData
GROUP BY id
HAVING COUNT(*) > 1;

SELECT * FROM dbo.TranformedData
WHERE reviews_per_month IS NULL;
