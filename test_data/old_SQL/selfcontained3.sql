
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_blabla
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT * FROM registeret_statistikk.kjoenn
WHERE rownum <= 100
)
;
-- SELECT * FROM ORA$PTT_blabla WHERE GYLDIGHETSTIDSPUNKT IS NOT NULL;

CREATE TABLE "blabla"
AS
(
SELECT *
FROM ORA$PTT_blabla
)
;

CREATE TABLE "blaBla"
AS
(
SELECT *
FROM ORA$PTT_blabla
)
;

-- SELECT DISTINCT TABLE_NAME FROM user_tab_columns;

SELECT COLUMN_NAME,DATA_TYPE,TABLE_NAME
FROM user_tab_columns
--WHERE table_name = '"blabla"'
WHERE table_name = 'blabla'
;

SELECT COLUMN_NAME,DATA_TYPE,TABLE_NAME
FROM user_tab_columns
WHERE table_name = (SELECT table_name FROM user_tables WHERE table_name LIKE '%blaBla%')
;




