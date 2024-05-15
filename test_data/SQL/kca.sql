--SELECT * FROM USER_TABLES;
--SELECT COUNT(*) FROM FORSKERDATA;
--SELECT * FROM FYLKESKODER;
-- SELECT * FROM FORSKERDATA WHERE rownum < 100;
-- Kobler først sammen dataene

-- 
-- For fødselsdato kan vi faktisk bruke gjeldende verdier
-- Lager aller først en tabell med gjeldende fødselsdato for samtlige
-- fødselsnummer som ligger i registererct
-- Sø
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_konstantverdier
ON COMMIT PRESERVE DEFINITION
AS
(
 SELECT 3 AS ant_kontroller,
 7000 as start_lopenr_kontroll
 FROM DUAL
)
;
-- DROP TABLE ORA$PTT_konstantverdier;
-- SELECT * FROM ORA$PTT_konstantverdier;
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
-- SELECT * FROM ORA$PTT_alle_fodselsdatoer;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_alle_fodselsdatoer;
-- SELECT COUNT(DISTINCT ENTITETSIDENTIFIKATOR) AS ant_rader FROM ORA$PTT_alle_fodselsdatoer;
--************** Start lager aller først kjønnshistorikk
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_all_kjonnshistorikk
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT DISTINCT fnr_dod.*,kjoenn.RAD,NVL(kjoenn.GYLDIGHETSTIDSPUNKT,fnr_dod.FOEDSELSDATO)
AS gyldig_fra,
CASE -- Renser pø at ugyldighetstidspunkt ikke skal vøre mindre enn gyldighetstidspunkt
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
-- Gjør "avkortning" mot evt. dødsfallsdato
)
;
-- DROP TABLE ORA$PTT_kjonn_fra_til;
--************** Slutt lager kjønnshistorikk
-- Kobler nø ogsø pø kjønn i tillegg til fødselsdag
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_FORSKERDATA_MED_FDATO
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT forskerdata.*,
ADD_MONTHS(fdatoer.FOEDSELSDATO, -1) AS tidligste_fdato, 
ADD_MONTHS(fdatoer.FOEDSELSDATO, 1) AS seneste_fdato,
kjonn_fra_til.kjoenn
FROM FORSKERDATA  forskerdata
INNER JOIN ORA$PTT_alle_fodselsdatoer fdatoer ON 
forskerdata."PasientFNr" = fdatoer.ENTITETSIDENTIFIKATOR
INNER JOIN ORA$PTT_kjonn_fra_til kjonn_fra_til ON
forskerdata."PasientFNr" = kjonn_fra_til.ENTITETSIDENTIFIKATOR  AND
forskerdata."date_match" >= kjonn_fra_til.gyldig_fra AND
forskerdata."date_match" < kjonn_fra_til.gyldig_til
)
;
-- DROP TABLE ORA$PTT_FORSKERDATA_MED_FDATO;
 -- SELECT * FROM ORA$PTT_FORSKERDATA_MED_FDATO;
-- DROP TABLE ORA$PTT_FORSKERDATA_MED_FDATO;
-- Lager en hjelpetabell med minste "min_fdato" og største "max_fdato" fra forskerdataene
-- Kan med redusere antall "mulige kontrollkandidater" fra ca 11,3 millioner til ca 5,4 millioner
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_tidligste_seneste_datoer
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT MIN(tidligste_fdato) AS aller_tidligste_fdato,
MAX(seneste_fdato) AS aller_seneste_fdato,
MIN("date_match") AS aller_tidligste_matchdato,
MAX("date_match") AS aller_seneste_matchdato
FROM ORA$PTT_FORSKERDATA_MED_FDATO
)
;
-- SELECT * FROM ORA$PTT_tidligste_seneste_datoer;
-- DROP TABLE ORA$PTT_aller_tidligste_seneste_fdato;
-- SELECT * FROM ORA$PTT_tidligste_seneste_datoer;
-- Ingen av pasientene kan vøre kontrollpersoner. Fjerner dem derfor her
-- Kobler nø ogsø pø eventuell dødsdato 
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_relevante_fodselsdatoer
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT alle_fdatoer.*
FROM ORA$PTT_alle_fodselsdatoer alle_fdatoer
INNER JOIN registeret_statistikk.identifikasjonsnummer ident
ON alle_fdatoer.ENTITETSIDENTIFIKATOR = ident.ENTITETSIDENTIFIKATOR AND
ident.ERGJELDENDE ='J'
LEFT JOIN FORSKERDATA forsker ON
forsker."PasientFNr" = alle_fdatoer.ENTITETSIDENTIFIKATOR
WHERE FOEDSELSDATO >= (SELECT MIN(tidligste_fdato) FROM ORA$PTT_FORSKERDATA_MED_FDATO)
AND FOEDSELSDATO <= (SELECT MAX(seneste_fdato) FROM ORA$PTT_FORSKERDATA_MED_FDATO) AND
-- ikke allerede død tidligere enn 
NVL(DOEDSDATO,SYSDATE) >= (
    SELECT ALLER_TIDLIGSTE_MATCHDATO FROM ORA$PTT_tidligste_seneste_datoer) AND 
forsker."PasientFNr" IS NULL
)
;
-- DROP TABLE ORA$PTT_relevante_fodselsdatoer;
-- SELECT * FROM ORA$PTT_relevante_fodselsdatoer ORDER BY ENTITETSIDENTIFIKATOR;
-- SELECT COUNT(*) AS ant_rader  FROM ORA$PTT_relevante_fodselsdatoer;
-- Skal sø fjerne alle som aldri har bodd i noen av de relevante fylkene
-- *********** Start fjerne fra "poolen" personer som aldri har bodd i Rogaland eller Vestland
-- DROP TABLE ORA$PTT_FORSKERDATAFYLKESNR;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_studiefylker
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT "currentCode" AS fylkesnr,'GJELDENDE' AS ER_GJELDENDE
FROM FORSKERDATA, FYLKESKODER fylke
WHERE LOWER(SUBSTR(fylke."newName", 1, LENGTH(FORSKERDATA."Fylkesnavn2020"))) = LOWER(FORSKERDATA."Fylkesnavn2020")
UNION
SELECT "oldCode" AS fylkesnr,'IKKE_GJELDENDE' AS ER_GJELDENDE
FROM FORSKERDATA, FYLKESKODER fylke
WHERE LOWER(SUBSTR(fylke."newName", 1, LENGTH(FORSKERDATA."Fylkesnavn2020"))) = LOWER(FORSKERDATA."Fylkesnavn2020") AND
fylke."oldCode" IS NOT NULL
)
;
-- DROP TABLE ORA$PTT_studiefylker;
-- SELECT * FROM ORA$PTT_studiefylker;
-- Start:  Lage  en hjelpetabell med relevante kommuner

-- SELECT DISTINCT KOMMUNENUMMER FROM registeret_statistikk.bostedsadresse;


-- SELECT * FROM ORA$PTT_studiekommuner;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_studiekommuner
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT DISTINCT KOMMUNENUMMER
FROM registeret_statistikk.bostedsadresse bo
INNER JOIN ORA$PTT_studiefylker fylker ON 
SUBSTR(KOMMUNENUMMER,1,2) = fylker.fylkesnr
)
;
-- DROP TABLE ORA$PTT_studiekommuner;
-- SELECT * FROM ORA$PTT_studiekommuner ORDER BY KOMMUNENUMMER;

-- TREKKER Ut alle 
-- Gjør dette i to omganger siden jeg vil unngø duplikater
-- Fjerner samtidig personer som var døde allerede ved studiestart (hvis personen
-- er død sø mø dødsdatoen vøre senere enn dette.
-- Sørger ogsø for ø ta bort alle adresseregistreringer registrert pø et senere tidspunkt
-- enn registrert dødsfall
-- SELECT * FROM ORA$PTT_relevante_fodselsdatoer WHERE rownum < 100; 
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_alle_relevant_bofylke
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT DISTINCT rel_fdatoer.ENTITETSIDENTIFIKATOR
FROM ORA$PTT_relevante_fodselsdatoer rel_fdatoer 
INNER JOIN registeret_statistikk.bostedsadresse bo ON 
rel_fdatoer.ENTITETSIDENTIFIKATOR = bo.ENTITETSIDENTIFIKATOR AND
NVL(bo.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO)  <=  NVL(DOEDSDATO,SYSDATE) 
INNER JOIN ORA$PTT_studiekommuner rel_kommuner ON 
bo.KOMMUNENUMMER = rel_kommuner.KOMMUNENUMMER
)
;
-- DROP TABLE ORA$PTT_alle_relevant_bofylke;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_alle_relevant_bofylke;
--  SELECT COUNT(DISTINCT ENTITETSIDENTIFIKATOR) FROM ORA$PTT_alle_relevant_bofylke;
-- SELECT * FROM registeret_statistikk.doedsfall WHERE rownum < 100;
-- SELECT * FROM ORA$PTT_relevante_fodselsdatoer WHERE rownum < 100;
-- 
--DROP TABLE ORA$PTT_basis;
-- *********** Slutt fjerne fra "poolen" personer som aldri har bodd i Rogaland eller Vestland 
-- eller døde ved studiestart (den siste gruppen var overraskende liten)

-- Fjerner sø "kode 6"o kode 7
-- CREATE 
-- Memo til selv: Oracle-SQL støtter ikke ø lage indeks pø midlertidige tabeller
-- Skal nø snart gjøre en "left join" med den enorme adressetabellen
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_addr_ikke_fortrolig
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT rel_fylke.*
FROM ORA$PTT_alle_relevant_bofylke rel_fylke LEFT JOIN 
registeret_statistikk.bostedsadresse bo ON 
rel_fylke.ENTITETSIDENTIFIKATOR = bo.ENTITETSIDENTIFIKATOR AND 
bo.adressegradering in ('KLIENTADRESSE','FORTROLIG')
WHERE bo.ENTITETSIDENTIFIKATOR IS NULL 
)
;
-- DROP TABLE ORA$PTT_addr_ikke_fortrolig;
--

-- Kobler pø igjen fødselsdato
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_basis
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT rel_fdato.*
FROM ORA$PTT_relevante_fodselsdatoer rel_fdato
INNER JOIN ORA$PTT_addr_ikke_fortrolig ikke_fortrolig ON 
rel_fdato.ENTITETSIDENTIFIKATOR = ikke_fortrolig.ENTITETSIDENTIFIKATOR
)
;
-- DROP TABLE ORA$PTT_basis;
-- SELECT * FROM ORA$PTT_basis WHERE rownum < 100;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_basis;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_addr_ikke_sensitiv;
-- SELECT * FROM ORA$PTT_addr_ikke_sensitiv WHERE rownum < 100;
-- SELECT * FROM ORA$PTT_alle_relevant_bofylke WHERE rownum < 100;
-- Memo til selv: Mø ikke gleme ø filtrere pø at selve fødselsnummeret mø vøre gjeldende
-- Memo til selv: Trenger Trenger ORA$PTT_studiekommuner
-- ********* Start slette tabeller
DROP TABLE ORA$PTT_alle_fodselsdatoer;
DROP TABLE ORA$PTT_all_kjonnshistorikk;
DROP TABLE ORA$PTT_kronologisk_kjonnshistorikk;
DROP TABLE ORA$PTT_tidligste_seneste_datoer;
DROP TABLE ORA$PTT_relevante_fodselsdatoer;
DROP TABLE ORA$PTT_studiefylker;
DROP TABLE ORA$PTT_alle_relevant_bofylke;
DROP TABLE ORA$PTT_addr_ikke_fortrolig;
-- ********* Slutt slette tabeller
--******************* Start lage historikktabeller
-- Ser pø bostedsadresse

-- SELECT * FROM ORA$PTT_basis WHERE rownum < 100;
-- SELECT * FROM ORA$PTT_studiekommuner;
-- "relevant_adressehistorikk" betyr bosted i relevant kommune
-- Sørger nø for at ikke henter inn noen adresseregistreringer med gyldighetstidspunkt
-- senere enn dødsdato til personen (hvis personen er død)
-- 
-- Memo til selv: For enkelte records kan ugyldighetspunkt vøre en tidligere dato
-- enn gyldighetstidspunkt. Tar nø høyde for det i koden over slik at ugyldighetstidspunkt
-- i slike tilfelller blir lik gyldighetstidspunkt. Dette er for at dette feltet
-- ikke skal ødelegge logikken min senere

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_all_addressehistorikk
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT bo.ENTITETSIDENTIFIKATOR,bo.RAD,NVL(bo.GYLDIGHETSTIDSPUNKT,basis.FOEDSELSDATO)
AS gyldig_fra,
CASE
    WHEN bo.UGYLDIGHETSTIDSPUNKT IS NOT NULL AND bo.GYLDIGHETSTIDSPUNKT IS NOT NULL AND
    bo.UGYLDIGHETSTIDSPUNKT < bo.GYLDIGHETSTIDSPUNKT THEN bo.GYLDIGHETSTIDSPUNKT
    ELSE bo.UGYLDIGHETSTIDSPUNKT
END AS UGYLDIGHETSTIDSPUNKT,
ERGJELDENDE,bo.AARSAK,bo.KOMMUNENUMMER,bo.ADRESSETYPE
FROM registeret_statistikk.bostedsadresse bo 
INNER JOIN ORA$PTT_basis basis ON
bo.ENTITETSIDENTIFIKATOR = basis.ENTITETSIDENTIFIKATOR AND
NVL(bo.GYLDIGHETSTIDSPUNKT,basis.FOEDSELSDATO) <= NVL(basis.DOEDSDATO,SYSDATE)
)
;
-- DROP TABLE ORA$PTT_all_addressehistorikk;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_all_addressehistorikk;
-- SELECT * FROM ORA$PTT_all_addressehistorikk  ORDER BY ENTITETSIDENTIFIKATOR,ERGJELDENDE DESC,gyldig_fra FETCH FIRST 200 ROWS ONLY;
-- "relevant_rad" er forskjellig fra "rad" ved at adressehistoriskk fra andre steder enn relevante kommunner er fjernet
-- Dette skaper selvfølgelig for mange et hull i historikken, men det er ikke her noe problem.
-- Feltet rad har verdien 0 for seneste rad og høyere verdier for tidligere rader, derfor sortert "DESC"
-- Ting ø tenke over bør sette inn fødselsdato
-- Memo til selv: Har konkludert med at det ikke nytter ø bruke 
-- feltet "ERGJELDENDE" for ø si noe om rekkefølge pø hendelser

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_kronologisk_adressehistorikk
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT  bo.*,
ROW_NUMBER() OVER (
               PARTITION BY ENTITETSIDENTIFIKATOR
               ORDER BY ENTITETSIDENTIFIKATOR,gyldig_fra,RAD DESC
               ) AS kronologisk_rad
FROM ORA$PTT_all_addressehistorikk bo 
)
;
-- DROP TABLE ORA$PTT_kronologisk_adressehistorikk;
-- SELECT * FROM ORA$PTT_kronologisk_adressehistorikk WHERE rownum < 100;
-- Gjør her ogsø "avkortning" av  "gyldig_til" mot eventuell dødsdato 

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_addresse_fra_til
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT forrige.*,
LEAST(
    NVL(neste.gyldig_fra,SYSDATE),
    NVL(forrige.UGYLDIGHETSTIDSPUNKT,SYSDATE),
    NVL(DOEDSDATO,SYSDATE) 
    )
AS gyldig_til
--
FROM ORA$PTT_kronologisk_adressehistorikk forrige 
LEFT JOIN ORA$PTT_kronologisk_adressehistorikk neste ON
forrige.ENTITETSIDENTIFIKATOR = neste.ENTITETSIDENTIFIKATOR AND 
neste.kronologisk_rad = forrige.kronologisk_rad + 1
-- Gjør "avkortning" mot evt. dødsfallsdato
LEFT JOIN registeret_statistikk.doedsfall dod 
ON forrige.ENTITETSIDENTIFIKATOR = dod.ENTITETSIDENTIFIKATOR AND
dod.ERGJELDENDE = 'J'
)
;
-- DROP TABLE ORA$PTT_addresse_fra_til;
-- SELECT * FROM ORA$PTT_addresse_fra_til WHERE rownum < 100 ORDER BY ENTITETSIDENTIFIKATOR,kronologisk_rad ;
-- Neste steg filtrerer pø at adressen er i en relevant kommune
-- Fjerner sø perioder der personene ikke er registrert i relevant kommune
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_relevant_adresse_fra_til
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT fra_til.*,fod.FOEDSELSDATO,fod.DOEDSDATO
FROM ORA$PTT_addresse_fra_til fra_til
INNER JOIN ORA$PTT_studiekommuner kommune ON 
fra_til.KOMMUNENUMMER = kommune.KOMMUNENUMMER  
-- Pøfører ogsø fødselsdato
INNER JOIN ORA$PTT_basis fod
ON fra_til.ENTITETSIDENTIFIKATOR = fod.ENTITETSIDENTIFIKATOR
-- Mø ogsø luke ut de med adressetype "ukjentBosted"
WHERE ADRESSETYPE IN ('vegadresse','matrikkeladresse')
)
;
-- DROP TABLE ORA$PTT_relevant_adresse_fra_til;
-- SELECT * FROM ORA$PTT_relevant_adresse_fra_til ORDER BY ENTITETSIDENTIFIKATOR,gyldig_fra;
-- Gjør en "avkortning" slik at "gyldig_til" for døde aldri
--SELECT COUNT(*) AS ant_rader 

-- DROP TABLE ORA$PTT_relevant_addresse_fra_til;
-- SELECT * FROM ORA$PTT_relevant_addresse_fra_til WHERE rownum <100;
-- SELECT DISTINCT ADRESSETYPE FROM ORA$PTT_relevant_addresse_fra_til;
-- Siden Oracle-SQL har en begrensning pø 16 "PRIVATE TEMPORARY"-tabeller sø 
-- mø jeg slette tabeller som jeg ikke lenger trenger.
--************* Start slette tabeller
DROP TABLE ORA$PTT_basis;
DROP TABLE ORA$PTT_studiekommuner;
DROP TABLE ORA$PTT_all_addressehistorikk;
DROP TABLE ORA$PTT_kronologisk_adressehistorikk;
DROP TABLE ORA$PTT_addresse_fra_til;
--************* Slutt slette tabeller
-- Memo til selv: Mø "tvinge igjennom" at utflytting skal senere enn "gyldig_fra" 
-- og tidligere enn "gyldig_til" skal føre til ny (tidligere) "gyldig_til"-dato
--  Bør egentlig ikke og finnes antagelig ingen slike tilfeller siden det ser ut som
-- om utflyttinger generelt fører til en "ugyldighetsdato" i adressehistorikken

-- *****************  Start  "Avkorting" av "gyldig_til" pga. utflytting
-- Lager en hjelpetabell
-- SELECT * FROM ORA$PTT_relevant_adresse_fra_til WHERE rownum < 100;
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_distinkte_fnr_dod
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT DISTINCT ENTITETSIDENTIFIKATOR,FOEDSELSDATO,DOEDSDATO
FROM ORA$PTT_relevant_adresse_fra_til
)
;
-- DROP TABLE ORA$PTT_distinkte_fnr_dod;
-- SELECT * FROM ORA$PTT_distinkte_fnr_dod where rownum < 100;
-- Lager ny hjelpetabell med evt dødsdato eller NULL
-- OBS Fjerner registreringer med gyldighetstidpunkter som er  senere enn dødsdato
-- Tar ogsø her hensyn til at ugyldighetstidpunkt kan vøre tidligere enn gyldighetstidspunkt
--SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_alle_inn_utflyttingstreff
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT fnr.*,
NVL(innflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO) AS gyldig_fra,
LEAST(
GREATEST(NVL(innflytt.UGYLDIGHETSTIDSPUNKT,SYSDATE),NVL(innflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO)),
NVL(DOEDSDATO,SYSDATE)
) AS avkortet_gyldig_til,
ERGJELDENDE,innflytt.RAD,'INNFLYTTING' AS migreringstype
FROM ORA$PTT_distinkte_fnr_dod fnr
INNER JOIN registeret_statistikk.innflytting innflytt ON
innflytt.ENTITETSIDENTIFIKATOR = fnr.ENTITETSIDENTIFIKATOR 
WHERE NVL(innflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO) <= NVL(DOEDSDATO,SYSDATE)
UNION
SELECT fnr.*,NVL(utflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO) AS gyldig_fra,
LEAST(
GREATEST(NVL(utflytt.UGYLDIGHETSTIDSPUNKT,SYSDATE),NVL(utflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO)),
NVL(DOEDSDATO,SYSDATE)
) AS avkortet_gyldig_til,
ERGJELDENDE,utflytt.RAD,'UTFLYTTING' AS migreringstype
FROM ORA$PTT_distinkte_fnr_dod fnr
INNER JOIN registeret_statistikk.utflytting utflytt ON
utflytt.ENTITETSIDENTIFIKATOR = fnr.ENTITETSIDENTIFIKATOR
WHERE NVL(utflytt.GYLDIGHETSTIDSPUNKT,FOEDSELSDATO) <= NVL(DOEDSDATO,SYSDATE)
)
;
-- DROP TABLE ORA$PTT_alle_inn_utflyttingstreff;
-- 
-- SELECT COUNT(*) FROM ORA$PTT_alle_inn_utflyttingstreff;
-- SELECT * FROM ORA$PTT_alle_inn_utflyttingstreff  ORDER BY ENTITETSIDENTIFIKATOR,gyldig_fra;

--Memo til selv: Er nødt til ø laga "gydlig_fra" har forrang over "er_gjeldende" siden
-- en senere registrert utvandring ikke nødvendigsvis fører til at registrert innvandring
-- slutter ø vøre "ERGJELDENDE"
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
--************ Start plukke ut tidsperiodene der personene bor i utlandet

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
--SELECT * FROM ORA$PTT_relevant_adresse_fra_til ORDER BY ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD FETCH FIRST 200 ROWS ONLY;

--  Fjerner utenlandsopphold fra bohistorikken
-- Memo til selv: Hvis viser seg at kjøretiden til while-løkken blir altfor lang sø før jeg prøve
-- gjøre all filtrering med  utenlandsperiodene før while. Dette vil i tilfelle
-- kreve en del programmering, sørlig tilfellene der et utenlandsopphold "splitter"
-- en adressehistorikkrecord
--
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_RELEVANTE_BOPERIODER
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT fra_til.ENTITETSIDENTIFIKATOR,fra_til.KRONOLOGISK_RAD,
CASE
    WHEN dekker_gyldig_fra.ENTITETSIDENTIFIKATOR IS NOT NULL 
        THEN dekker_gyldig_fra.slutt_boperiode_utland
    ELSE fra_til.gyldig_fra
END AS gyldig_fra,
CASE
    WHEN dekker_gyldig_til.ENTITETSIDENTIFIKATOR IS NOT NULL 
        THEN dekker_gyldig_til.start_boperiode_utland
    ELSE fra_til.gyldig_til
END AS gyldig_til
--
FROM ORA$PTT_relevant_adresse_fra_til fra_til 
LEFT JOIN ORA$PTT_boperiode_utland heldekkende ON
heldekkende.ENTITETSIDENTIFIKATOR = fra_til.ENTITETSIDENTIFIKATOR AND
heldekkende.start_boperiode_utland <= fra_til.GYLDIG_FRA AND
heldekkende.slutt_boperiode_utland >= fra_til.GYLDIG_TIL 
LEFT JOIN ORA$PTT_boperiode_utland dekker_gyldig_fra ON
dekker_gyldig_fra.ENTITETSIDENTIFIKATOR = fra_til.ENTITETSIDENTIFIKATOR AND
dekker_gyldig_fra.start_boperiode_utland <= fra_til.GYLDIG_FRA AND
dekker_gyldig_fra.slutt_boperiode_utland  > fra_til.GYLDIG_FRA AND
dekker_gyldig_fra.slutt_boperiode_utland < fra_til.GYLDIG_TIL 

LEFT JOIN ORA$PTT_boperiode_utland dekker_gyldig_til ON
dekker_gyldig_til.ENTITETSIDENTIFIKATOR = fra_til.ENTITETSIDENTIFIKATOR AND
dekker_gyldig_til.start_boperiode_utland > fra_til.GYLDIG_FRA AND
dekker_gyldig_til.start_boperiode_utland <  fra_til.GYLDIG_TIL AND 
dekker_gyldig_til.slutt_boperiode_utland > fra_til.GYLDIG_TIL 
WHERE heldekkende.ENTITETSIDENTIFIKATOR  IS NULL
)
;
-- DROP TABLE ORA$PTT_RELEVANTE_BOPERIODER;
-- SELECT * FROM ORA$PTT_RELEVANTE_BOPERIODER ORDER BY ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD FETCH FIRST 200 ROWS ONLY;
-- SELECT COUNT(*) AS ant_rader FROM  ORA$PTT_RELEVANTE_BOPERIODER;
-- SELECT COUNT(DISTINCT ENTITETSIDENTIFIKATOR) AS ant_rader FROM  ORA$PTT_RELEVANTE_BOPERIODER;
-- Memo til selv Skal sø "kollapse" historikk pø boperioder basert pø gammel "FHI-kode".

--************** Start "kollapse bohistorikk" 
-- Skal sø "kollapse" bohistorikken. Tanken her er at neste rad har samme "gyldig_fra"-dato
-- som forrige "gyldig_til" sø er det en fortsettelse av samme boperiode, og de to radene kan
-- derfor sløs sammen til en rad med gyldig_fra lik "forrige.gyldig_fra" og gyldig_til lik
-- "neste.gydlig_til"
-- Lager en hjelpevariable "flagg" pø om det er et "tidsgap" mellom etterfølgende rader til samme
-- person
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
neste.gyldig_fra < forrige.gyldig_til   + INTERVAL '1' DAY 
)
;
-- DROP TABLE ORA$PTT_flagg_boavbrudd;
-- SELECT * FROM ORA$PTT_flagg_boavbrudd ORDER BY ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD FETCH FIRST 200 ROWS ONLY;
-- Lager sø løpenummer for "boforløp" ved hjelp av "cumsum" av flaggvariabelen "boavbrudd".
-- SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;
-- SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_boforlop
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD,GYLDIG_FRA,GYLDIG_TIL,
SUM(boavbrudd) OVER(PARTITION BY ENTITETSIDENTIFIKATOR
				ORDER BY KRONOLOGISK_RAD) AS lopenr_boforlop
FROM ORA$PTT_flagg_boavbrudd
)
;
-- DROP TABLE ORA$PTT_boforlop;
-- SELECT * FROM ORA$PTT_boforlop ORDER BY ENTITETSIDENTIFIKATOR,KRONOLOGISK_RAD FETCH FIRST 200 ROWS ONLY;
-- SELECT DISTINCT ENTITETSIDENTIFIKATOR FROM ORA$PTT_boforlop WHERE LOPENR_BOFORLOP > 1;
CREATE PRIVATE TEMPORARY TABLE ORA$PTT_boforlop_fra_til
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT ENTITETSIDENTIFIKATOR,lopenr_boforlop,MIN(GYLDIG_FRA) AS GYLDIG_FRA,
MAX(GYLDIG_TIL) AS GYLDIG_TIL
FROM ORA$PTT_boforlop
GROUP BY ENTITETSIDENTIFIKATOR,lopenr_boforlop
)
;
-- DROP TABLE ORA$PTT_boforlop_fra_til;
-- SELECT TABLE_NAME FROM USER_PRIVATE_TEMP_TABLES;
-- SELECT MAX(lopenr_boforlop) AS max_lopernr FROM ORA$PTT_boforlop_fra_til;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_boforlop_fra_til;
-- SELECT COUNT(DISTINCT ENTITETSIDENTIFIKATOR) FROM ORA$PTT_boforlop_fra_til;
--************** Slutt "kollapse bohistorikk" 

-- Mø koble pø fødselsdato og kjønn igjen

-- SELECT * FROM ORA$PTT_DISTINKTE_FNR_DOD;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_boforlop_fdato
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT bo.*,fnr_dod.FOEDSELSDATO
FROM ORA$PTT_boforlop_fra_til bo
INNER JOIN ORA$PTT_DISTINKTE_FNR_DOD fnr_dod
ON bo.ENTITETSIDENTIFIKATOR = fnr_dod.ENTITETSIDENTIFIKATOR
)
;
-- DROP TABLE ORA$PTT_boforlop_fdato;
-- SELECT * FROM ORA$PTT_boforlop_fdato WHERE rownum < 100;
-- TRENGER ORA$PTT_BOPERIODE_UTLAND;
-- *********** Start sletter noen tabeller igjen
DROP TABLE ORA$PTT_FLAGG_BOAVBRUDD;
DROP TABLE ORA$PTT_RELEVANTE_BOPERIODER;
DROP TABLE ORA$PTT_KRONOLOGISK_MIGRASJON;
DROP TABLE ORA$PTT_ALLE_INN_UTFLYTTINGSTREFF;
DROP TABLE ORA$PTT_RELEVANT_ADRESSE_FRA_TIL;
DROP TABLE ORA$PTT_boforlop;
DROP TABLE ORA$PTT_boforlop_fra_til;
-- *********** Slutt sletter noen tabeller igjen

--SELECT start_lopenr_kontroll FROM  ORA$PTT_konstantverdier;
-- OBSSSSSSSSSSSSSSS Mø her hardkode startverdien til løpenummeret
-- Memo til selv:  Midlertidige tabeller har ikke støtte for "IDENTITY" for løpenummer.
-- Lager derfor PASIENTKONTROLLER som persistent/fast tabell

-- SELECT * FROM ORA$PTT_FORSKERDATA_MED_FDATO WHERE rownum < 100;
-- Memo til selv: En av pasientene har to "date_match"-er
CREATE TABLE PASIENTKONTROLLER (
    PasientFNr  VARCHAR2(20),
    date_match  DATE,
	Kontroll_Fnr VARCHAR2(20),   
    id NUMBER GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1 MINVALUE 0)
    )
    ;
-- DROP TABLE PASIENTKONTROLLER;



CREATE PRIVATE TEMPORARY TABLE ORA$PTT_datematches ( 
       PasientFNr VARCHAR2(20),
       date_match DATE,
       KJOENN VARCHAR2(20),
       TIDLIGSTE_FDATO DATE,
       SENESTE_FDATO DATE
        ) ON COMMIT PRESERVE DEFINITION
        ;
-- 
-- DROP TABLE ORA$PTT_datematches;
-- Bestilleren ønsker ikke at samme kontroll skal kobles til mer enn en pasient
-- Memo til selv: "NUMBER(x)" er integer, "NUMBER(x,y)" er desimaltall
DECLARE
  PasientFNr VARCHAR2(20); -- Adjust the data type to match the column's data type
  KJOENN VARCHAR2(20);
  date_match DATE;
  TIDLIGSTE_FDATO DATE;
  SENESTE_FDATO DATE;
  sql_stmt VARCHAR2(2000); -- Skal inneholde mesteparten av  sql -koden
  sql_date_match_test VARCHAR2(1000);
  lopenr_kontroll NUMBER(10);  
BEGIN
   lopenr_kontroll := 7000;
  FOR rec IN (SELECT "PasientFNr" AS PasientFNr,KJOENN,"date_match" AS date_match,
  TIDLIGSTE_FDATO,SENESTE_FDATO FROM ORA$PTT_FORSKERDATA_MED_FDATO) LOOP    
    PasientFNr := rec.PasientFNr;
    KJOENN:= rec.KJOENN;
    date_match:= rec.date_match;
    TIDLIGSTE_FDATO:= rec.TIDLIGSTE_FDATO;
    SENESTE_FDATO:= rec.SENESTE_FDATO;   
    sql_date_match_test := '
    INSERT INTO ORA$PTT_datematches(PasientFNr,date_match,KJOENN,TIDLIGSTE_FDATO,SENESTE_FDATO)   
        SELECT ''' || PasientFNr || ''' AS PasientFNr, 
        TO_DATE(''' || TO_CHAR(date_match, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AS date_match,
        ''' || KJOENN || ''' AS PasientFNr,
        TO_DATE(''' || TO_CHAR(TIDLIGSTE_FDATO, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AS TIDLIGSTE_FDATO,
        TO_DATE(''' || TO_CHAR(SENESTE_FDATO, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AS SENESTE_FDATO
        FROM DUAL
    '
    ;
    sql_stmt := '
        INSERT INTO PASIENTKONTROLLER(PasientFNr,date_match,Kontroll_Fnr)   
        SELECT ''' || PasientFNr || ''' AS PasientFNr,
        TO_DATE(''' || TO_CHAR(date_match, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AS date_match,
        bo.ENTITETSIDENTIFIKATOR AS Kontroll_Fnr
        FROM ORA$PTT_boforlop_fdato bo 
        INNER JOIN ORA$PTT_KJONN_FRA_TIL kjoenn ON
        bo.ENTITETSIDENTIFIKATOR = kjoenn.ENTITETSIDENTIFIKATOR AND 
        kjoenn.GYLDIG_FRA <= TO_DATE(''' || TO_CHAR(date_match, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AND 
        kjoenn.GYLDIG_TIL > TO_DATE(''' || TO_CHAR(date_match, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'')        
        LEFT JOIN ORA$PTT_BOPERIODE_UTLAND bo_utland ON
        bo.ENTITETSIDENTIFIKATOR =  bo_utland.ENTITETSIDENTIFIKATOR AND
        bo_utland.START_BOPERIODE_UTLAND <= TO_DATE(''' || TO_CHAR(date_match, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AND 
        bo_utland.SLUTT_BOPERIODE_UTLAND > TO_DATE(''' || TO_CHAR(date_match, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') 
        LEFT JOIN PASIENTKONTROLLER kontroller  ON
        bo.ENTITETSIDENTIFIKATOR = kontroller.Kontroll_Fnr        
        WHERE bo.FOEDSELSDATO >=  TO_DATE(''' || TO_CHAR(TIDLIGSTE_FDATO, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AND
            bo.FOEDSELSDATO <  TO_DATE(''' || TO_CHAR(SENESTE_FDATO, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AND
            bo.GYLDIG_FRA <= TO_DATE(''' || TO_CHAR(date_match, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AND 
            bo.GYLDIG_TIL > TO_DATE(''' || TO_CHAR(date_match, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') AND
            kjoenn.KJOENN = ''' || KJOENN || ''' AND
            kontroller.Kontroll_Fnr IS NULL AND 
            bo_utland.ENTITETSIDENTIFIKATOR IS NULL
        ORDER BY DBMS_RANDOM.VALUE
        FETCH FIRST (SELECT ant_kontroller FROM ORA$PTT_konstantverdier) ROWS ONLY
        '
        ;
    EXECUTE IMMEDIATE sql_date_match_test;
    EXECUTE IMMEDIATE 'COMMIT';
    EXECUTE IMMEDIATE sql_stmt; 
    DBMS_OUTPUT.PUT_LINE('Value: ' || PasientFNr);
    EXECUTE IMMEDIATE 'COMMIT';
  END LOOP;
END;
/
-- END STRUCTURE
-- DROP TABLE ORA$PTT_datematches;
-- DROP TABLE PASIENTKONTROLLER;
-- SELECT * FROM ORA$PTT_datematches;  -FøR NULL!!!!!!!!!!
-- SELECT COUNT(*) AS ant_rader FROM PASIENTKONTROLLER;
-- SELECT * FROM PASIENTKONTROLLER;
-- SELECT COUNT(DISTINCT PASIENTFNR) AS ant_pasienter FROM PASIENTKONTROLLER;
-- SELECT COUNT(DISTINCT Kontroll_Fnr) AS ant_kontroller FROM PASIENTKONTROLLER;
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_FORSKERDATA_MED_FDATO;
-- SELECT COUNT(*) AS ant_Rader FROM ORA$PTT_datematches;   
-- Legger sø til startverdien til lopenummer som 

-- SELECT COUNT(*) AS ant_rader FROM FORSKERDATA;

CREATE PRIVATE TEMPORARY TABLE ORA$PTT_pasientkontroller_med_lopenummer
ON COMMIT PRESERVE DEFINITION
AS
(
SELECT forsker.*,
(ID + (SELECT start_lopenr_kontroll FROM ORA$PTT_konstantverdier)) AS lopenr_kontroll,
Kontroll_Fnr
FROM FORSKERDATA forsker
LEFT JOIN PASIENTKONTROLLER ON 
forsker."PasientFNr" = PASIENTKONTROLLER.PASIENTFNR AND
forsker."date_match" = PASIENTKONTROLLER.DATE_MATCH
)
;
-- DROP TABLE ORA$PTT_pasientkontroller_med_lopenummer;
-- SELECT * FROM FORSKERDATA WHERE rownum < 100;
-- SELECT * FROM ORA$PTT_pasientkontroller_med_lopenummer ORDER BY lopenr_kontroll FETCH FIRST 200 ROWS ONLY;
-- SELECT * FROM ORA$PTT_pasientkontroller_med_lopenummer ORDER BY lopenr_kontroll;
-- SELECT COUNT(*) AS ant_rader FROM PASIENTKONTROLLER; 
-- SELECT COUNT(*) AS ant_rader FROM ORA$PTT_pasientkontroller_med_lopenummer; 
-- SELECT COUNT(*) AS ant_rader FROM FORSKERDATA;
-- SELECT * FROM ORA$PTT_pasientkontroller_med_lopenummer WHERE Kontroll_Fnr IS NULL;
--DROP TABLE ORA$PTT_PASIENTKONTROLLER;
-- SELECT * FROM ORA$PTT_PASIENTKONTROLLER ORDER BY PasientFNr FETCH FIRST 200 ROWS ONLY;
-- 

-- SELECT COUNT(DISTINCT "PasientFNr") AS ant_pasienter FROM FORSKERDATA;
-- SELECT COUNT(DISTINCT "pid") AS ant_pasienter FROM FORSKERDATA;
DROP TABLE PASIENTKONTROLLER;
-- Legger uttrekket i en fast tabell slik at kan 
CREATE TABLE Forskeruttrekk
AS
(
SELECT *
FROM ORA$PTT_pasientkontroller_med_lopenummer 
)
;
-- DROP TABLE Forskeruttrekk;
-- SELECT COUNT(*) AS ant_rader FROM Forskeruttrekk;
--SELECT TABLE_NAME FROM USER_TABLES;
;
