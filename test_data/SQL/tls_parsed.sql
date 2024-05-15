CREATE PRIVATE TEMPORARY TABLE ORA$PTT_forskerdata_fnr
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT fod.ENTITETSIDENTIFIKATOR,fod.FOEDSELSDATO,forsker."patientid"
FROM FORSKERDATA forsker
LEFT JOIN  registeret_statistikk.foedsel fod
ON TO_DATE(SUBSTR("patientid",1,8),'YYYYMMDD') = fod.FOEDSELSDATO AND
fod.ENTITETSIDENTIFIKATOR LIKE '%' ||''|| SUBSTR("patientid",9,5)
)
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_GJELDENDE_ADRESSE
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT forsker.*,status.STATUS,kjonn.KJOENN,
navn.FORNAVN, navn.MELLOMNAVN, navn.ETTERNAVN,
CASE WHEN navn.MELLOMNAVN IS NOT NULL AND LENGTH(navn.MELLOMNAVN) > 0 THEN
navn.FORNAVN || ' ' || navn.MELLOMNAVN || ' ' || navn.ETTERNAVN
ELSE navn.FORNAVN || ' ' || navn.ETTERNAVN
END AS fullt_navn,
bo.adressenavn,bo.husnummer,bo.husbokstav,bo.poststedsnavn,
bo.postnummer,    bo.adressenavn ||' '|| bo.husnummer ||' '|| bo.husbokstav as ADRESSE
FROM ORA$PTT_forskerdata_fnr forsker
LEFT JOIN registeret_statistikk.identifikasjonsnummer ident ON
forsker.ENTITETSIDENTIFIKATOR = ident.ENTITETSIDENTIFIKATOR AND
ident.ergjeldende in ('J')
LEFT JOIN registeret_statistikk.personstatus status ON
status.ENTITETSIDENTIFIKATOR = ident.ENTITETSIDENTIFIKATOR AND
status.ergjeldende in ('J')
LEFT JOIN registeret_statistikk.kjoenn kjonn ON
kjonn.ENTITETSIDENTIFIKATOR = ident.ENTITETSIDENTIFIKATOR  AND
kjonn.ergjeldende in ('J')
LEFT JOIN registeret_statistikk.NAVN navn ON
navn.ENTITETSIDENTIFIKATOR = ident.ENTITETSIDENTIFIKATOR AND
navn.ergjeldende in ('J')
LEFT JOIN  registeret_statistikk.bostedsadresse bo ON
bo.ENTITETSIDENTIFIKATOR = ident.ENTITETSIDENTIFIKATOR  AND
bo.ergjeldende in ('J') AND UPPER(bo.adressegradering) in ('UGRADERT')
)
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_POSTADRESSE
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT bostedsaddr.*,
CASE
WHEN register_stat.POSTADRESSE_FORMAT IS  NULL THEN NULL
WHEN  register_stat.POSTADRESSE_FORMAT = 'postadresseIFrittFormat' THEN
post.POSTADRESSEIFRITTFORMAT_DATA
WHEN register_stat.POSTADRESSE_FORMAT = 'postboksadresse' THEN
post.POSTBOKSADRESSE_DATA
WHEN register_stat.POSTADRESSE_FORMAT = 'vegadresse' THEN
POST.VEGADRESSE_DATA
END AS postadresse
FROM ORA$PTT_GJELDENDE_ADRESSE bostedsaddr
LEFT JOIN registeret_statistikk.register_statistikk register_stat ON
bostedsaddr.ENTITETSIDENTIFIKATOR = register_stat.IDENTIFIKATOR
AND register_stat.POSTADRESSE_FORMAT IS NOT NULL
LEFT JOIN registeret_statistikk.postadresse post ON
post.ENTITETSIDENTIFIKATOR = register_stat.IDENTIFIKATOR AND
post.ERGJELDENDE in ('J')
)
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_forskeruttrekk
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT * FROM ORA$PTT_POSTADRESSE
)
