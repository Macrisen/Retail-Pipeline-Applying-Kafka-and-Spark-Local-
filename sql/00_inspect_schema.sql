-- Optional: compare server columns with the supplied schemas before installation.
SELECT table_schema,table_name,column_name,data_type,is_nullable
FROM information_schema.columns
WHERE table_schema IN ('bronze','silver','gold')
ORDER BY table_schema,table_name,ordinal_position;
