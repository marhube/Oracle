--SELECT DISTINCT "Pasient_Fylkesnummer_Per2020" ,"Pasient_Fylkesnummer"
--FROM KRITERIEFIL 
--ORDER BY "Pasient_Fylkesnummer_Per2020","Pasient_Fylkesnummer"
--;
-- Definerer her noen konstanter
-- SELECT TABLE_NAME FROM USER_TABLES;
-- SELECT * FROM KOMMUNEKODER;
-- Lager en tabell som mapper hver kommune til dagens fylke
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_kommune_fylke_korrespondanse
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT DISTINCT NVL("oldCode","currentCode") AS knr,SUBSTR("currentCode",1,2) AS FYLKESNR
FROM  KOMMUNEKODER
)
;
--  SELECT * FROM ORA$PTT_kommune_fylke_korrespondanse;
--  SELECT * FROM ORA$PTT_kommune_fylke_korrespondanse WHERE KNR = '5012';
--  SELECT * FROM KOMMUNEKODER WHERE "oldCode" = '5012';
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_kommune_fylke_korrespondanse;
-- SELECT COUNT(DISTINCT KNR) AS ant_rader FROM ORA$PTT_kommune_fylke_korrespondanse;

-- SELECT COUNT(*) AS ant_rader FROM KOMMUNEKODER WHERE "oldCode" IS NOT NULL;
-- SELECT COUNT(DISTINCT "oldCode") AS ant_rader FROM KOMMUNEKODER WHERE "oldCode" IS NOT NULL;
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_konstantverdier
ON COMMIT PRESERVE DEFINITION
AS
(
 SELECT 5 AS ant_kontroller,6 AS max_ant_mnd_ulik_alder
 FROM DUAL
)
;

-- DROP TABLE ORA$PTT_konstantverdier;
-- SELECT * FROM ORA$PTT_konstantverdier;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_forskerdata
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT krit.*,nokkel."Pasient_Identitetsnummer"
FROM KRITERIEFIL krit
INNER JOIN 
NOKKELFILPASIENT nokkel 
ON krit."Pasient_Lopenummer" = nokkel."Pasient_Lopenummer"
)
;
-- DROP TABLE ORA$PTT_forskerdata;
-- SELECT * FROM ORA$PTT_forskerdata WHERE rownum < 100;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_forskerdata;
-- SELECT * FROM ORA$PTT_forskerdata WHERE "Pasient_Fylkesnummer" IS NULL;
-- Trekker f�rst ut alle f�dselsdatoer
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_alle_fodselsdatoer 
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT fod.ENTITETSIDENTIFIKATOR,fod.FOEDSELSDATO,dod.DOEDSDATO
FROM registeret_statistikk.foedsel fod
LEFT JOIN registeret_statistikk.doedsfall dod 
ON fod.ENTITETSIDENTIFIKATOR = dod.ENTITETSIDENTIFIKATOR AND
dod.ERGJELDENDE IN ('J') 
WHERE fod.ERGJELDENDE IN ('J') 
)
;
-- DROP TABLE ORA$PTT_alle_fodselsdatoer;
-- SELECT * FROM ORA$PTT_alle_fodselsdatoer WHERE rownum < 200;
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_all_kjonnshistorikk
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT DISTINCT fnr_dod.*,kjoenn.RAD,NVL(kjoenn.GYLDIGHETSTIDSPUNKT,fnr_dod.FOEDSELSDATO)
AS gyldig_fra,
CASE -- Renser p� at ugyldighetstidspunkt ikke skal v�re mindre enn gyldighetstidspunkt
    WHEN kjoenn.UGYLDIGHETSTIDSPUNKT IS NOT NULL AND kjoenn.GYLDIGHETSTIDSPUNKT IS NOT NULL AND
    kjoenn.UGYLDIGHETSTIDSPUNKT < kjoenn.GYLDIGHETSTIDSPUNKT THEN kjoenn.GYLDIGHETSTIDSPUNKT
    ELSE kjoenn.UGYLDIGHETSTIDSPUNKT
END AS UGYLDIGHETSTIDSPUNKT,
--
kjoenn.ERGJELDENDE,
kjoenn.KJOENN
FROM ORA$PTT_alle_fodselsdatoer fnr_dod
INNER JOIN registeret_statistikk.kjoenn 
ON fnr_dod.ENTITETSIDENTIFIKATOR = kjoenn.ENTITETSIDENTIFIKATOR AND
NVL(kjoenn.GYLDIGHETSTIDSPUNKT,fnr_dod.FOEDSELSDATO) <= NVL(fnr_dod.DOEDSDATO,SYSDATE)
)
;
-- DROP TABLE ORA$PTT_all_kjonnshistorikk;
-- SELECT * FROM ORA$PTT_all_kjonnshistorikk ORDER BY ENTITETSIDENTIFIKATOR,RAD FETCH FIRST 200 ROWS ONLY;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_kronologisk_kjonnshistorikk
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT  kjoenn.*,
ROW_NUMBER() OVER (
               PARTITION BY ENTITETSIDENTIFIKATOR
               ORDER BY ENTITETSIDENTIFIKATOR,gyldig_fra,RAD DESC
               ) AS kronologisk_rad
FROM ORA$PTT_all_kjonnshistorikk kjoenn 
)
;
-- DROP TABLE ORA$PTT_kronologisk_kjonnshistorikk;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_kjonn_fra_til
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT forrige.*,
LEAST(
    NVL(neste.gyldig_fra,SYSDATE),
    NVL(forrige.UGYLDIGHETSTIDSPUNKT,SYSDATE),
    NVL(forrige.DOEDSDATO,SYSDATE) 
    )
AS gyldig_til
--
FROM ORA$PTT_kronologisk_kjonnshistorikk forrige 
LEFT JOIN ORA$PTT_kronologisk_kjonnshistorikk neste ON
forrige.ENTITETSIDENTIFIKATOR = neste.ENTITETSIDENTIFIKATOR AND 
neste.kronologisk_rad = forrige.kronologisk_rad + 1
-- Gj�r "avkortning" mot evt. d�dsfallsdato
)
;
-- DROP TABLE ORA$PTT_kjonn_fra_til;
--************** Slutt lager kj�nnshistorikk
-- SELECT * FROM ORA$PTT_kjonn_fra_til ORDER BY ENTITETSIDENTIFIKATOR FETCH FIRST 200 ROWS ONLY;
-- SELECT * FROM ORA$PTT_forskerdata WHERE rownum < 100;
-- Kobler n� ogs� p� kj�nn i tillegg til f�dselsdag
--Pasient_Identitetsnummer
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_FORSKERDATA_MED_FDATO
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT forskerdata.*,fdatoer.FOEDSELSDATO,
EXTRACT(YEAR FROM fdatoer.FOEDSELSDATO) AS fodselsaar,
ADD_MONTHS(fdatoer.FOEDSELSDATO, -1* (SELECT max_ant_mnd_ulik_alder FROM ORA$PTT_konstantverdier)) AS tidligste_fdato, 
ADD_MONTHS(fdatoer.FOEDSELSDATO, (SELECT max_ant_mnd_ulik_alder FROM ORA$PTT_konstantverdier)) AS seneste_fdato,
kjonn_fra_til.kjoenn
FROM ORA$PTT_forskerdata  forskerdata
INNER JOIN ORA$PTT_alle_fodselsdatoer fdatoer ON 
forskerdata."Pasient_Identitetsnummer" = fdatoer.ENTITETSIDENTIFIKATOR
INNER JOIN ORA$PTT_kjonn_fra_til kjonn_fra_til ON
forskerdata."Pasient_Identitetsnummer" = kjonn_fra_til.ENTITETSIDENTIFIKATOR  AND
forskerdata."Utlevering_Dato" >= kjonn_fra_til.gyldig_fra AND
forskerdata."Utlevering_Dato" < kjonn_fra_til.gyldig_til
)
;
-- DROP TABLE ORA$PTT_FORSKERDATA_MED_FDATO;
-- SELECT * FROM ORA$PTT_FORSKERDATA_MED_FDATO WHERE rownum < 100;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_tidligste_seneste_datoer
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT MIN(tidligste_fdato) AS aller_tidligste_fdato,
MAX(seneste_fdato) AS aller_seneste_fdato,
MIN("Utlevering_Dato") AS aller_tidligste_matchdato,
MAX("Utlevering_Dato") AS aller_seneste_matchdato
FROM ORA$PTT_FORSKERDATA_MED_FDATO
)
;
-- DROP TABLE ORA$PTT_tidligste_seneste_datoer;
-- SELECT * FROM ORA$PTT_tidligste_seneste_datoer;

-- Plukker ut alle med f�dselsdato innenfor tidsspennet
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_alle_relevant_fdato
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT DISTINCT alle_fdatoer.ENTITETSIDENTIFIKATOR
FROM  ORA$PTT_alle_fodselsdatoer alle_fdatoer
INNER JOIN registeret_statistikk.identifikasjonsnummer ident
ON alle_fdatoer.ENTITETSIDENTIFIKATOR = ident.ENTITETSIDENTIFIKATOR AND
ident.ERGJELDENDE ='J' 
WHERE FOEDSELSDATO >= (SELECT MIN(tidligste_fdato) FROM ORA$PTT_FORSKERDATA_MED_FDATO)
AND FOEDSELSDATO <= (SELECT MAX(seneste_fdato) FROM ORA$PTT_FORSKERDATA_MED_FDATO) AND
-- ikke allerede d�d tidligere enn 
NVL(DOEDSDATO,SYSDATE) >= (
    SELECT ALLER_TIDLIGSTE_MATCHDATO FROM ORA$PTT_tidligste_seneste_datoer)
)
;
-- SELECT DISTINCT adressegradering FROM registeret_statistikk.bostedsadresse;
-- DROP TABLE ORA$PTT_alle_relevant_fdato;
-- Fjerner her ogs� alle med kode6 og kode7
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_addr_ikke_fortrolig
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT alle_fdatoer.*
FROM ORA$PTT_alle_relevant_fdato alle_fdatoer
LEFT JOIN registeret_statistikk.bostedsadresse bo ON 
alle_fdatoer.ENTITETSIDENTIFIKATOR = bo.ENTITETSIDENTIFIKATOR AND 
bo.adressegradering <> 'UGRADERT'
WHERE bo.ENTITETSIDENTIFIKATOR IS NULL 
)
;
-- DROP TABLE ORA$PTT_addr_ikke_fortrolig;
-- SELECT * FROM ORA$PTT_addr_ikke_fortrolig WHERE rownum < 200;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_addr_ikke_fortrolig;
--SELECT DISTINCT adressegradering 
--FROM registeret_statistikk.bostedsadresse bo
--INNER JOIN ORA$PTT_addr_ikke_fortrolig ikke_fortrolig ON
--ikke_fortrolig.ENTITETSIDENTIFIKATOR = bo.ENTITETSIDENTIFIKATOR
--;

--
-- Kobler p� igjen f�dselsdato
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_basis
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT alle_fdatoer.*
FROM ORA$PTT_alle_fodselsdatoer alle_fdatoer 
INNER JOIN ORA$PTT_addr_ikke_fortrolig ikke_fortrolig ON 
alle_fdatoer.ENTITETSIDENTIFIKATOR = ikke_fortrolig.ENTITETSIDENTIFIKATOR
)
;

-- SELECT * FROM ORA$PTT_basis WHERE ROWNUM < 100;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_basis;
-- SELECT COUNT(DISTINCT ENTITETSIDENTIFIKATOR) AS ant_rader FROM ORA$PTT_basis;
-- SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;
-- ************* Start lage tabell med "start_botid_utland" og "slutt_botid_utland"
-- OBS Fjerner registreringer med gyldighetstidpunkter som er  senere enn d�dsdato
-- Tar ogs� her hensyn til at ugyldighetstidpunkt kan v�re tidligere enn gyldighetstidspunkt

-- Memo til selv: M�ten jeg tenker p� personstatus "BOSATT" for historikkuttrekk
-- er at man har status "BOSATT" for en dato hvis datoen er
-- 1. Senere eller lik f�dselsdato
-- 2. Tidligere enn evt, registrert d�dsdato
-- 3. Ikke utvandret p� denne datoen
-- 4. Ikke ukjent bosted p� denne datoen

-- I denne sammenhengen �nsker jeg at "bosatt" skal v�re temmelig sikkert "bosatt".
-- Jeg bearbeider derfor dataene slik at jeg plukker ut 
-- perioder der personen sannsynligvis er i utlandet og heller lager disse periodene'
-- for lange enn  for korte, slik at hvis personen er "kanskje utvandret" p� en dato
-- s� f�r ikke vedkommende status bosatt. Ulempen med denne tiln�rmingen er at "botid_utland"
-- kanskje overdriver litt tiden folk er i utlandet. 
-- Hvis form�let er � plukke ut "sikre utenlandsperioder" s� man gj�re ting
-- litt annerledes
-- START SLETTE TABELLER
DROP TABLE ORA$PTT_forskerdata;
DROP TABLE ORA$PTT_all_kjonnshistorikk;
DROP TABLE ORA$PTT_kronologisk_kjonnshistorikk;
DROP TABLE ORA$PTT_alle_relevant_fdato;
DROP TABLE ORA$PTT_addr_ikke_fortrolig;
-- SLUTT SLETTE TABELLER


CREATE PRIVATE TEMPORARY TABLE ORA$PTT_bearbeidet_utflytting
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT basis.ENTITETSIDENTIFIKATOR,UGYLDIGHETSTIDSPUNKT,
CASE
    WHEN GYLDIGHETSTIDSPUNKT IS NULL AND UTFLYTTINGSDATO IS NULL 
        THEN  basis.FOEDSELSDATO
    WHEN GYLDIGHETSTIDSPUNKT IS NULL AND UTFLYTTINGSDATO IS NOT NULL
        THEN UTFLYTTINGSDATO
    WHEN GYLDIGHETSTIDSPUNKT IS NOT NULL AND UTFLYTTINGSDATO IS NULL
        THEN GYLDIGHETSTIDSPUNKT
    WHEN GYLDIGHETSTIDSPUNKT IS NOT NULL AND UTFLYTTINGSDATO IS NOT NULL
        THEN LEAST(GYLDIGHETSTIDSPUNKT,UTFLYTTINGSDATO)
END AS gyldig_fra,
ERGJELDENDE,RAD
FROM ORA$PTT_basis basis
INNER JOIN registeret_statistikk.utflytting utflytt ON
utflytt.ENTITETSIDENTIFIKATOR = basis.ENTITETSIDENTIFIKATOR
)
;
-- DROP TABLE ORA$PTT_bearbeidet_utflytting;
-- SELECT * FROM ORA$PTT_bearbeidet_utflytting WHERE rownum < 100;
--SELECT * 
--FROM ORA$PTT_bearbeidet_utflytting 
--ORDER BY ENTITETSIDENTIFIKATOR,GYLDIG_FRA 
--FETCH FIRST 50 ROWS ONLY
--;
--
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_alle_inn_utflyttingstreff
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT basis.*,
NVL(innflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO) AS gyldig_fra,
LEAST(
GREATEST(NVL(innflytt.UGYLDIGHETSTIDSPUNKT,SYSDATE),NVL(innflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO)),
NVL(DOEDSDATO,SYSDATE)
) AS avkortet_gyldig_til,
ERGJELDENDE,innflytt.RAD,'INNFLYTTING' AS migreringstype
FROM ORA$PTT_basis basis
INNER JOIN registeret_statistikk.innflytting innflytt ON
innflytt.ENTITETSIDENTIFIKATOR = basis.ENTITETSIDENTIFIKATOR 
WHERE NVL(innflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO) <= NVL(DOEDSDATO,SYSDATE)
UNION
SELECT basis.*,gyldig_fra,
LEAST(
GREATEST(NVL(utflytt.UGYLDIGHETSTIDSPUNKT,SYSDATE),utflytt.gyldig_fra),
NVL(DOEDSDATO,SYSDATE)
) AS avkortet_gyldig_til,
ERGJELDENDE,utflytt.RAD,'UTFLYTTING' AS migreringstype
FROM ORA$PTT_basis basis
INNER JOIN ORA$PTT_bearbeidet_utflytting utflytt ON
utflytt.ENTITETSIDENTIFIKATOR = basis.ENTITETSIDENTIFIKATOR
WHERE utflytt.gyldig_fra <= NVL(DOEDSDATO,SYSDATE) -- sanity check
)
;
-- DROP TABLE  ORA$PTT_alle_inn_utflyttingstreff;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_alle_inn_utflyttingstreff;
-- SELECT * FROM ORA$PTT_alle_inn_utflyttingstreff ORDER BY ENTITETSIDENTIFIKATOR,GYLDIG_FRA FETCH FIRST 200 ROWS ONLY;

--Memo til selv: Er n�dt til � la "gydlig_fra" ha forrang over "er_gjeldende" siden
-- en senere registrert utvandring ikke n�dvendigsvis f�rer til at registrert innvandring
-- slutter � v�re "ERGJELDENDE"
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_kronologisk_migrasjon
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT  inn_ut.*,
ROW_NUMBER() OVER (
               PARTITION BY ENTITETSIDENTIFIKATOR
               ORDER BY ENTITETSIDENTIFIKATOR, gyldig_fra,ERGJELDENDE DESC
               ) AS kronologisk_rad
FROM ORA$PTT_alle_inn_utflyttingstreff inn_ut 
)
;
-- DROP TABLE ORA$PTT_kronologisk_migrasjon;
-- SELECT *  FROM ORA$PTT_kronologisk_migrasjon ORDER BY ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD FETCH FIRST 200 ROWS ONLY;
--
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_migrasjon_fra_til
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT forrige.*,
LEAST(forrige.avkortet_gyldig_til,NVL(neste.gyldig_fra,SYSDATE))
AS gyldig_til
--
FROM ORA$PTT_kronologisk_migrasjon forrige 
LEFT JOIN ORA$PTT_kronologisk_migrasjon neste ON
forrige.ENTITETSIDENTIFIKATOR = neste.ENTITETSIDENTIFIKATOR AND 
neste.kronologisk_rad = forrige.kronologisk_rad + 1
)
;
-- DROP TABLE ORA$PTT_migrasjon_fra_til;
-- SELECT * FROM ORA$PTT_migrasjon_fra_til ORDER BY ENTITETSIDENTIFIKATOR DESC,kronologisk_rad FETCH FIRST 200 ROWS ONLY;
-- START SLETTE TABELLER
DROP TABLE ORA$PTT_alle_inn_utflyttingstreff;
DROP TABLE ORA$PTT_kronologisk_migrasjon;
-- SLUTT SLETTE TABELLER



--************ Start plukke ut tidsperiodene der personene bor i utlandet

-- SELECT * FROM registeret_statistikk.utflytting WHERE rownum < 10;
-- Tar n� ogs� med flyttedato
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_boperiode_utland
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD,
CASE
    WHEN KRONOLOGISK_RAD = 1 THEN FOEDSELSDATO
    ELSE GYLDIG_FRA 
END AS start_boperiode_utland,
--
CASE
    WHEN MIGRERINGSTYPE = 'INNFLYTTING' THEN GYLDIG_FRA
    ELSE GYLDIG_TIL
END AS slutt_boperiode_utland
FROM ORA$PTT_migrasjon_fra_til fra_til
WHERE (MIGRERINGSTYPE = 'INNFLYTTING' AND KRONOLOGISK_RAD = 1 AND GYLDIG_FRA > FOEDSELSDATO)  OR
    MIGRERINGSTYPE = 'UTFLYTTING'
)
;
 -- DROP TABLE ORA$PTT_boperiode_utland;
-- SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;
-- SELECT * FROM ORA$PTT_boperiode_utland ORDER BY ENTITETSIDENTIFIKATOR,start_boperiode_utland,slutt_boperiode_utland FETCH FIRST 200 ROWS ONLY;
-- ************* Slutt lage tabell med "start_botid_utland" og "slutt_botid_utland"
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_basis basis;
-- Memo til selv: Venter n� litt med � "mappe" til dagens fylke
-- Planen er
-- SELECT * FROM ORA$PTT_kommune_fylke_korrespondanse WHERE rownum < 100;
-- Etter dette steget s� skal "gylidg_fra" aldri v�re NULL
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_all_adressehistorikk
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT bo.ENTITETSIDENTIFIKATOR,bo.RAD,
COALESCE(bo.GYLDIGHETSTIDSPUNKT,bo.FLYTTEDATO,basis.FOEDSELSDATO)
AS gyldig_fra,
CASE 
    WHEN bo.UGYLDIGHETSTIDSPUNKT IS NOT NULL AND
        COALESCE(bo.GYLDIGHETSTIDSPUNKT,bo.FLYTTEDATO) IS NOT NULL AND 
        bo.UGYLDIGHETSTIDSPUNKT < COALESCE(bo.GYLDIGHETSTIDSPUNKT,bo.FLYTTEDATO) 
        THEN COALESCE(bo.GYLDIGHETSTIDSPUNKT,bo.FLYTTEDATO)
    ELSE bo.UGYLDIGHETSTIDSPUNKT
END AS UGYLDIGHETSTIDSPUNKT,
bo.ADRESSETYPE,
bo.FLYTTEDATO,
basis.FOEDSELSDATO,
basis.DOEDSDATO,
ERGJELDENDE,bo.AARSAK,bo.KOMMUNENUMMER,korrespond.FYLKESNR
FROM registeret_statistikk.bostedsadresse bo 
INNER JOIN ORA$PTT_basis basis ON
bo.ENTITETSIDENTIFIKATOR = basis.ENTITETSIDENTIFIKATOR AND
NVL(bo.GYLDIGHETSTIDSPUNKT,basis.FOEDSELSDATO) <= NVL(basis.DOEDSDATO,SYSDATE)
INNER JOIN ORA$PTT_kommune_fylke_korrespondanse korrespond   ON 
bo.KOMMUNENUMMER =  korrespond.KNR
)
;
-- DROP TABLE ORA$PTT_all_adressehistorikk;
-- SELECT * FROM ORA$PTT_all_adressehistorikk WHERE rownum < 10;
-- SELECT * FROM ORA$PTT_basis WHERE rownum < 10;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_all_addressehistorikk;

-- Memo til selv: Har konkludert med at det ikke nytter � bruke 
-- feltet "ERGJELDENDE" for � si noe om rekkef�lge p� hendelser

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_bearbeidet_adressehistorikk 
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT bo.*,
CASE 
    WHEN FLYTTEDATO IS NOT NULL THEN LEAST(gyldig_fra,FLYTTEDATO)
    ELSE gyldig_fra
END  AS min_gyldig_fra_flyttedato,
CASE 
    WHEN FLYTTEDATO IS NOT NULL THEN GREATEST(gyldig_fra,FLYTTEDATO)
    ELSE gyldig_fra
END  AS max_gyldig_fra_flyttedato
FROM ORA$PTT_all_adressehistorikk bo
)
;
-- DROP TABLE ORA$PTT_bearbeidet_adressehistorikk;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_kronologisk_adressehistorikk
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT  bo.*,
ROW_NUMBER() OVER (
               PARTITION BY ENTITETSIDENTIFIKATOR
               ORDER BY ENTITETSIDENTIFIKATOR,min_gyldig_fra_flyttedato,RAD DESC
               ) AS kronologisk_rad
FROM ORA$PTT_bearbeidet_adressehistorikk bo 
)
;
-- DROP TABLE ORA$PTT_kronologisk_adressehistorikk;
-- SELECT * FROM ORA$PTT_kronologisk_adressehistorikk WHERE rownum < 100;
-- Gj�r her ogs� "avkortning" av  "gyldig_til" mot eventuell d�dsdato 
-- SELECT * FROM ORA$PTT_BASIS WHERE rownum < 10;
-- Gj�r "avkortning" mot evt. d�dsfallsdato (kan hente doedsdato fra basis)
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_adresse_fra_til
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT forrige.*,
LEAST(
    COALESCE(neste.min_gyldig_fra_flyttedato,SYSDATE),
    COALESCE(forrige.UGYLDIGHETSTIDSPUNKT,SYSDATE),
    COALESCE(forrige.DOEDSDATO,SYSDATE) 
    )
AS gyldig_til
--
FROM ORA$PTT_kronologisk_adressehistorikk forrige 
LEFT JOIN ORA$PTT_kronologisk_adressehistorikk neste ON
forrige.ENTITETSIDENTIFIKATOR = neste.ENTITETSIDENTIFIKATOR AND 
neste.kronologisk_rad = forrige.kronologisk_rad + 1
)
;
-- DROP TABLE ORA$PTT_adresse_fra_til;
-- SELECT * FROM ORA$PTT_adresse_fra_til ORDER BY ENTITETSIDENTIFIKATOR, GYLDIG_FRA;
-- Luker ut ukjent bosted og utenlandsperiode
-- SELECT * FROM ORA$PTT_relevante_fodselsdatoer WHERE rownum < 100;
-- Luker ut  adressetype "ukjentBosted"
-- SELECT * FROM ORA$PTT_addresse_fra_til WHERE rownum < 100;
-- SELECT COUNT(*) As
--SELECT * FROM ORA$PTT_FORSKERDATA_MED_FDATO WHERE rownum < 100;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_FORSKERDATA_MED_FDATO WHERE "Pasient_Fylkesnummer" IS NULL;
-- Kobler p� fylkesnummer v
-- DROP TABLE ORA$PTT_FORSKERDATA_MED_FREG_FYLKE;
-- SELECT * FROM ORA$PTT_FORSKERDATA_MED_FREG_FYLKE WHERE rownum < 100;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_FORSKERDATA_MED_FREG_FYLKE WHERE FREG_FYLKE IS NULL;


-- Bruker herfra og ut  "max_gyldig_fra_flyttedato som ny "gyldig_fra"
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_RELEVANTE_BOPERIODER
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT fra_til.ENTITETSIDENTIFIKATOR,fra_til.FOEDSELSDATO,fra_til.DOEDSDATO,
fra_til.FYLKESNR,fra_til.KRONOLOGISK_RAD,
CASE
    WHEN dekker_max_gyldig_fra_flyttedato.ENTITETSIDENTIFIKATOR IS NOT NULL 
        THEN dekker_max_gyldig_fra_flyttedato.slutt_boperiode_utland
    ELSE fra_til.max_gyldig_fra_flyttedato
END AS gyldig_fra,
CASE
    WHEN dekker_gyldig_til.ENTITETSIDENTIFIKATOR IS NOT NULL 
        THEN dekker_gyldig_til.start_boperiode_utland
    ELSE fra_til.gyldig_til
END AS gyldig_til
--
FROM ORA$PTT_adresse_fra_til fra_til
LEFT JOIN ORA$PTT_boperiode_utland heldekkende ON
heldekkende.ENTITETSIDENTIFIKATOR = fra_til.ENTITETSIDENTIFIKATOR AND
heldekkende.start_boperiode_utland <= fra_til.max_gyldig_fra_flyttedato AND
heldekkende.slutt_boperiode_utland >= fra_til.GYLDIG_TIL 
LEFT JOIN ORA$PTT_boperiode_utland dekker_max_gyldig_fra_flyttedato ON
dekker_max_gyldig_fra_flyttedato.ENTITETSIDENTIFIKATOR = fra_til.ENTITETSIDENTIFIKATOR AND
dekker_max_gyldig_fra_flyttedato.start_boperiode_utland <= fra_til.max_gyldig_fra_flyttedato AND
dekker_max_gyldig_fra_flyttedato.slutt_boperiode_utland  > fra_til.max_gyldig_fra_flyttedato AND
dekker_max_gyldig_fra_flyttedato.slutt_boperiode_utland < fra_til.GYLDIG_TIL 
LEFT JOIN ORA$PTT_boperiode_utland dekker_gyldig_til ON
dekker_gyldig_til.ENTITETSIDENTIFIKATOR = fra_til.ENTITETSIDENTIFIKATOR AND
dekker_gyldig_til.start_boperiode_utland > fra_til.max_gyldig_fra_flyttedato AND
dekker_gyldig_til.start_boperiode_utland <  fra_til.GYLDIG_TIL AND
dekker_gyldig_til.slutt_boperiode_utland >= fra_til.GYLDIG_TIL
WHERE gyldig_til > max_gyldig_fra_flyttedato  AND 
ADRESSETYPE IN ('vegadresse','matrikkeladresse') AND
heldekkende.ENTITETSIDENTIFIKATOR  IS NULL
)
;
-- DROP TABLE ORA$PTT_RELEVANTE_BOPERIODER;
-- SELECT * FROM ORA$PTT_RELEVANTE_BOPERIODER WHERE rownum < 100;
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_flagg_boavbrudd
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT neste.*,
CASE
    WHEN forrige.ENTITETSIDENTIFIKATOR IS NULL THEN 1
    ELSE 0
END AS boavbrudd
FROM ORA$PTT_RELEVANTE_BOPERIODER neste 
LEFT JOIN ORA$PTT_RELEVANTE_BOPERIODER forrige
ON forrige.ENTITETSIDENTIFIKATOR = neste.ENTITETSIDENTIFIKATOR AND
neste.KRONOLOGISK_RAD = forrige.KRONOLOGISK_RAD  +1  AND
neste.gyldig_fra < forrige.gyldig_til   + INTERVAL '1' DAY  AND 
neste.FYLKESNR = forrige.FYLKESNR
)
;
-- DROP TABLE ORA$PTT_flagg_boavbrudd;
-- SELECT * FROM ORA$PTT_flagg_boavbrudd ORDER BY ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD FETCH FIRST 200 ROWS ONLY;
-- Lager s� l�penummer for "boforl�p" ved hjelp av "cumsum" av flaggvariabelen "boavbrudd".
-- SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;
-- SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;

-- M� n� ta en ny runde med � slette tabeller
DROP TABLE ORA$PTT_migrasjon_fra_til;
DROP TABLE ORA$PTT_ALLE_FODSELSDATOER;
DROP TABLE ORA$PTT_all_adressehistorikk;
DROP TABLE ORA$PTT_kronologisk_adressehistorikk;
DROP TABLE ORA$PTT_ADRESSE_FRA_TIL;
DROP TABLE ORA$PTT_bearbeidet_adressehistorikk;
DROP TABLE ORA$PTT_RELEVANTE_BOPERIODER;
DROP TABLE ORA$PTT_KOMMUNE_FYLKE_KORRESPONDANSE;
DROP TABLE ORA$PTT_TIDLIGSTE_SENESTE_DATOER;


CREATE PRIVATE TEMPORARY TABLE ORA$PTT_boforlop
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT boavbrudd.*,
SUM(boavbrudd) OVER(PARTITION BY ENTITETSIDENTIFIKATOR
				ORDER BY KRONOLOGISK_RAD) AS lopenr_boforlop
FROM ORA$PTT_flagg_boavbrudd boavbrudd
)
;
-- DROP TABLE ORA$PTT_boforlop;
-- SELECT * FROM ORA$PTT_boforlop ORDER BY ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD FETCH FIRST 200 ROWS ONLY;
-- SELECT DISTINCT ENTITETSIDENTIFIKATOR FROM ORA$PTT_boforlop WHERE LOPENR_BOFORLOP > 1;
-- Memo til selv: Innen for samme "lopenr_boforlop" skal det ikke v�re endringer i fylke
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_boforlop_fra_til
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT ENTITETSIDENTIFIKATOR,FYLKESNR,lopenr_boforlop,MIN(GYLDIG_FRA) AS GYLDIG_FRA,
MAX(GYLDIG_TIL) AS GYLDIG_TIL
FROM ORA$PTT_boforlop
GROUP BY ENTITETSIDENTIFIKATOR,FYLKESNR,lopenr_boforlop
)
;
-- DROP TABLE ORA$PTT_boforlop_fra_til;
-- SELECT * FROM ORA$PTT_boforlop_fra_til  WHERE rownum <  10;

-- Kobler s� p� fodselsdato og evt doedsdato
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_boforlop_fdato
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT bo.*,basis.FOEDSELSDATO,basis.DOEDSDATO
FROM ORA$PTT_boforlop_fra_til bo
INNER JOIN ORA$PTT_BASIS basis ON
bo.ENTITETSIDENTIFIKATOR = basis.ENTITETSIDENTIFIKATOR
)
;
-- DROP TABLE ORA$PTT_boforlop_fdato;
-- SELECT * FROM ORA$PTT_boforlop_fra_til WHERE rownum < 20;
-- SELECT * FROM ORA$PTT_boforlop_fdato WHERE rownum < 100;

-- DROP TABLE ORA$PTT_boforlop_fra_til;
-- SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;
-- SELECT * FROM ORA$PTT_BASIS WHERE rownum < 100;
-- DROP TABLE ORA$PTT_boperiode_utland;
--*********** Ny liten sletterunde
DROP TABLE ORA$PTT_flagg_boavbrudd;
DROP TABLE ORA$PTT_boforlop;


--  Lager all
-- SELECT * FROM ORA$PTT_FORSKERDATA_MED_FDATO WHERE rownum < 10;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_alle_relevante_kjoenn_fylker
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT DISTINCT "Pasient_Fylkesnummer_Per2020" AS FYLKESNR,
"Pasient_Fylkesnavn_Per2020" AS FYLKESNAVN, KJOENN
FROM ORA$PTT_FORSKERDATA_MED_FDATO
WHERE "Pasient_Fylkesnummer_Per2020" IS NOT NULL AND KJOENN IS NOT NULL
)
;
-- DROP TABLE ORA$PTT_alle_relevante_kjoenn_fylker;
-- SELECT * FROM ORA$PTT_alle_relevante_kjoenn_fylker;
-- -- OBSSSSSSSSS Pasienten m� ikke f� seg selv som kontroll !!!!!!!!!!
-- Start fjerne pasienter fra "populasjonen" som kontroller skal trekkes fra
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_boforlop_uten_pasienter
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT bo.* 
FROM ORA$PTT_boforlop_fdato bo
LEFT JOIN NOKKELFILPASIENT nokkel ON 
bo.ENTITETSIDENTIFIKATOR = nokkel."Pasient_Identitetsnummer" 
WHERE nokkel."Pasient_Identitetsnummer" IS NULL
)
;
-- DROP TABLE ORA$PTT_boforlop_uten_pasienter;
-- Fjerner ogs�
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_kjonn_uten_pasienter
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT kjonn.* 
FROM ORA$PTT_KJONN_FRA_TIL kjonn
LEFT JOIN NOKKELFILPASIENT nokkel ON 
kjonn.ENTITETSIDENTIFIKATOR = nokkel."Pasient_Identitetsnummer" 
WHERE nokkel."Pasient_Identitetsnummer" IS NULL
)
;
-- DROP TABLE ORA$PTT_kjonn_uten_pasienter;
-- Slutt fjerne pasienter fra "populasjonen" som kontroller skal trekkes fra
-- SELECT * FROM ORA$PTT_FORSKERDATA_MED_FDATO WHERE rownum < 100;
-- DROP TABLE ORA$PTT_alle_relevante_kjoenn_fylker;
-- SELECT * FROM ORA$PTT_alle_relevante_kjoenn_fylker ORDER BY FYLKESNR,KJOENN;
-- SELECT * FROM ORA$PTT_FORSKERDATA_MED_FDATO WHERE rownum < 10;
-- SELECT * FROM ORA$PTT_boforlop_fra_til WHERE rownum < 10;
-- Skal iterere gjennom to l�kker.
-- Den ytterste l�kken itererer gjennom relevante fylke og trekker ut
-- den delen av bohistorikken og records i forskerdata som er relatert til iteratorfylke 
-- SELECT * FROM ORA$PTT_FORSKERDATA_MED_FDATO WHERE rownum < 20;
-- SELECT * FROM ORA$PTT_boforlop_fdato WHERE rownum < 20;
-- SELECT * FROM ORA$PTT_KJONN_FRA_TIL WHERE rownum < 20;


-- Itererer n� i ytre l�kke p� alle unike kombinasjoner av fylke og kj�nn
-- SELECT * FROM ORA$PTT_boforlop_fdato WHERE rownum < 100;

-- OBSSSSSSSSSSS M� definere schema til alle hjelpetabeller utenfor lokkene for at de skal bli
-- synlige i den indre l�kken! Veldig irriterende!!!!!
--
CREATE TABLE BOHISTORIKK_ITERATORFYLKE
    AS
    (
    SELECT *
    FROM ORA$PTT_boforlop_uten_pasienter bo
    WHERE rownum < 0    
    ) 
; 
-- DROP TABLE BOHISTORIKK_ITERATORFYLKE;
-- SELECT * FROM BOHISTORIKK_ITERATORFYLKE;
CREATE TABLE HISTORIKK_ITERATORKJONN
    AS
    (
    SELECT *
    FROM ORA$PTT_kjonn_uten_pasienter kjoenn
    WHERE rownum < 0    
    ) 
;
-- DROP TABLE HISTORIKK_ITERATORKJONN;
-- SELECT * FROM HISTORIKK_ITERATORKJONN;
-- SELECT * FROM ORA$PTT_KJONN_FRA_TIL WHERE rownum < 0;
--
CREATE TABLE ITERATOR_FORSKERDATA
    AS
    (
    SELECT "Pasient_Lopenummer" AS Pasient_Lopenummer,
    "Pasient_Identitetsnummer" AS pasient_Fnr,
    "Utlevering_Dato" AS utleveringsdato,
    TIDLIGSTE_FDATO,
    SENESTE_FDATO
    FROM ORA$PTT_FORSKERDATA_MED_FDATO forsker
    WHERE rownum < 0
    ) 
;
-- DROP TABLE ITERATOR_FORSKERDATA;
 -- SELECT * FROM ORA$PTT_iterator_forskerdata;
-- SELECT * FROM ORA$PTT_konstantverdier;

-- DROP TABLE ORA$PTT_pasienter_i_fylke;
-- Har n� ikke lenger noen "nullable"-kolonne
CREATE TABLE PASIENTKONTROLLER (
    Pasient_Lopenummer VARCHAR2(20),
    Pasient_Fnr VARCHAR2(20),
    utleveringsdato DATE,
	Kontroll_Fnr VARCHAR2(20),
    KJOENN  VARCHAR2(10),
    FYLKESNR VARCHAR2(10),
    FYLKESNAVN VARCHAR2(30)    
    )
    ;
-- DROP TABLE PASIENTKONTROLLER;
-- DELETE FROM PASIENTKONTROLLER;
-- SELECT *  FROM ORA$PTT_boforlop_uten_pasienter WHERE rownum < 10;
-- SELECT * FROM ORA$PTT_boforlop_fdato;


-- SELECT * FROM ORA$PTT_boforlop_fdato WHERE rownum < 100;
-- OBSSSSSSSSS Pasienten m� ikke f� seg selv som kontroll !!!!!!!!!!

-- OBSSSSSSSSS Pasienten m� ikke f� seg selv som kontroll !!!!!!!!!!
DECLARE
BEGIN
  FOR outer_rec IN (SELECT FYLKESNR,FYLKESNAVN,KJOENN FROM ORA$PTT_alle_relevante_kjoenn_fylker) LOOP
  -- Start "nullstiller hjelpetabellene"
  DELETE FROM BOHISTORIKK_ITERATORFYLKE;
  DELETE FROM HISTORIKK_ITERATORKJONN;
  DELETE FROM ITERATOR_FORSKERDATA;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Er n� inne i indre l�kke');
  COMMIT;
  -- Slutt "nullstiller hjelpetabellene"
  INSERT INTO BOHISTORIKK_ITERATORFYLKE
  SELECT bo.*
  FROM ORA$PTT_boforlop_uten_pasienter bo
  LEFT JOIN PASIENTKONTROLLER pk ON 
  bo.ENTITETSIDENTIFIKATOR =  pk.Kontroll_Fnr -- 
  WHERE bo.FYLKESNR = outer_rec.FYLKESNR  AND
  pk.Kontroll_Fnr IS NULL
  ;
  INSERT INTO HISTORIKK_ITERATORKJONN
  SELECT kjoenn.*
  FROM ORA$PTT_kjonn_uten_pasienter kjoenn
  LEFT JOIN PASIENTKONTROLLER pk ON 
  kjoenn.ENTITETSIDENTIFIKATOR =  pk.Kontroll_Fnr 
  WHERE kjoenn.KJOENN = outer_rec.KJOENN  AND
  pk.Kontroll_Fnr IS NULL
  ;
  --
  INSERT INTO ITERATOR_FORSKERDATA
  SELECT "Pasient_Lopenummer" AS Pasient_Lopenummer,
    "Pasient_Identitetsnummer" AS Pasient_Fnr,
    "Utlevering_Dato" AS utleveringsdato,
    TIDLIGSTE_FDATO,
    SENESTE_FDATO
  FROM ORA$PTT_FORSKERDATA_MED_FDATO forsker
  WHERE forsker."Pasient_Fylkesnummer_Per2020" = outer_rec.FYLKESNR  AND  
  forsker.KJOENN = outer_rec.KJOENN
  ;
  COMMIT;  
  -- Starter indre l�kke her
    FOR inner_rec IN (SELECT * FROM ITERATOR_FORSKERDATA) LOOP 
      INSERT INTO PASIENTKONTROLLER(Pasient_Lopenummer,Pasient_Fnr,utleveringsdato,
      Kontroll_Fnr,KJOENN,FYLKESNR,FYLKESNAVN)   
      SELECT inner_rec.Pasient_Lopenummer,inner_rec.Pasient_Fnr,inner_rec.utleveringsdato,
      bo.ENTITETSIDENTIFIKATOR AS Kontroll_Fnr,outer_rec.KJOENN,outer_rec.FYLKESNR,outer_rec.FYLKESNAVN
      FROM BOHISTORIKK_ITERATORFYLKE bo 
      INNER JOIN HISTORIKK_ITERATORKJONN kjoenn ON
        bo.ENTITETSIDENTIFIKATOR = kjoenn.ENTITETSIDENTIFIKATOR AND 
            kjoenn.GYLDIG_FRA <= inner_rec.utleveringsdato AND 
            kjoenn.GYLDIG_TIL > inner_rec.utleveringsdato
      LEFT JOIN ORA$PTT_BOPERIODE_UTLAND bo_utland ON
        bo.ENTITETSIDENTIFIKATOR =  bo_utland.ENTITETSIDENTIFIKATOR AND
        bo_utland.START_BOPERIODE_UTLAND <= inner_rec.utleveringsdato  AND 
        bo_utland.SLUTT_BOPERIODE_UTLAND > inner_rec.utleveringsdato
      LEFT JOIN PASIENTKONTROLLER kontroller  ON
        bo.ENTITETSIDENTIFIKATOR = kontroller.Kontroll_Fnr  
      WHERE bo.FOEDSELSDATO >=   inner_rec.TIDLIGSTE_FDATO AND
            bo.FOEDSELSDATO <  inner_rec.SENESTE_FDATO AND
            bo.GYLDIG_FRA <= inner_rec.utleveringsdato AND 
            bo.GYLDIG_TIL > inner_rec.utleveringsdato AND
            kontroller.Kontroll_Fnr IS NULL AND 
            bo_utland.ENTITETSIDENTIFIKATOR IS NULL  
        ORDER BY DBMS_RANDOM.VALUE
        FETCH FIRST (SELECT ant_kontroller FROM ORA$PTT_konstantverdier) ROWS ONLY
        ;      
      COMMIT;
    END LOOP;
  END LOOP;
END; 
/
--
