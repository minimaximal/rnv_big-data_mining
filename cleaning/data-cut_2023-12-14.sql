-- delete all unwanted data points for clean data cut

DELETE
FROM stops
WHERE (api_plannedDeparture like '2023-12-15%')
   OR (api_realtimeDeparture like '2023-12-15%');

DELETE stops
FROM stops
         JOIN journeys ON stops.api_journey = journeys.id
WHERE journeys.api_fistStop_plannedDeparture LIKE '2023-12-15%';

DELETE
FROM journeys
WHERE journeys.id NOT IN (SELECT api_journey
                          FROM stops);


-- to check result after deletion

SELECT *
FROM stops
ORDER BY api_plannedDeparture DESC;

SELECT *
FROM stops
ORDER BY api_realtimeDeparture DESC;

SELECT *
FROM journeys
ORDER BY api_fistStop_plannedDeparture DESC;