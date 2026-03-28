-- Please edit the sample below

CREATE MATERIALIZED VIEW users AS
SELECT
    user_id,
    email,
    name,
    user_type
FROM samples.wanderbricks.users;