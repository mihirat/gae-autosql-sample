SELECT
 weight_pounds, state, year, gestation_weeks
FROM
 publicdata:samples.natality
ORDER BY weight_pounds DESC LIMIT 100;
