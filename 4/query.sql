SELECT
    phrase,
    arrayFilter(x -> ((x.2) > 0), groupArray((hour, views_diff))) AS views_by_hour
FROM
(
    SELECT
        phrase,
        toHour(dt) AS hour,
        max(views) - min(views) AS views_diff
    FROM phrases_views
    GROUP BY
        phrase,
        hour
    ORDER BY
        phrase ASC,
        hour DESC
)
GROUP BY phrase;