DELETE FROM result.source_count
WHERE time < NOW() - (INTERVAL '2 days');

DELETE FROM result.fail_count
WHERE time < NOW() - (INTERVAL '2 days');

DELETE FROM result.category_count
WHERE time < NOW() - (INTERVAL '2 days');

DELETE FROM result.gender_count
WHERE time < NOW() - (INTERVAL '2 days');

DELETE FROM result.revenue
WHERE time < NOW() - (INTERVAL '2 days');