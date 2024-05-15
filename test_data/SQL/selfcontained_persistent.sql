
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_blabla
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT * FROM registeret_statistikk.kjoenn
WHERE rownum <= 100
)
;

CREATE TABLE BLABLA
AS
(
SELECT *
FROM ORA$PTT_blabla
)
;
-- DROP TABLE "blaBla";

-- SELECT TABLE_NAME FROM USER_TABLES;