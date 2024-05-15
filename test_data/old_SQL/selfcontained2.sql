
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
--
SELECT COLUMN_NAME,DATA_TYPE,TABLE_NAME
FROM user_tab_columns
--WHERE table_name = '"blabla"'
;


