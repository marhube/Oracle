
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_blabla
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT * FROM registeret_statistikk.kjoenn
WHERE rownum <= 100
)
;
