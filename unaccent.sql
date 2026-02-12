CREATE OR REPLACE TABLE t_unaccent(
    `key` String,
    `value` String
)
ENGINE = MergeTree
ORDER BY `key`;

INSERT INTO t_unaccent
SELECT splitByChar('\t', line)[1] AS `key`, splitByChar('\t', line)[2] AS `value`
FROM url('https://raw.githubusercontent.com/postgres/postgres/5b148706c5c8ffffe5662fe569a0f0bcef2351d9/contrib/unaccent/unaccent.rules',
    'LineAsString',
    'line String');

CREATE DICTIONARY unaccent (
    `key` String,
    `value` String
)
PRIMARY KEY `key`
SOURCE(CLICKHOUSE(
    TABLE 't_unaccent' DB 'default' USER 'default' PASSWORD 'eHfi~ASpRAh~6'
))
LAYOUT(HASHED())
LIFETIME(3600);


CREATE OR REPLACE FUNCTION splitUTF8 AS (x) ->
    arrayMap((i) -> substringUTF8(x, i+1, 1), range(lengthUTF8(x)));

CREATE OR REPLACE FUNCTION removeDiacritics AS (s) ->
    arrayStringConcat(arrayMap(x -> dictGetOrDefault('unaccent', 'value', x, x), ngrams(s, 1)));

CREATE OR REPLACE FUNCTION removeDiacritics AS (s) ->
    arrayStringConcat(
        arrayMap((i) -> dictGetOrDefault('unaccent', 'value', substringUTF8(s, i+1, 1), substringUTF8(s, i+1, 1)),
        range(lengthUTF8(s))));

SELECT removeDiacritics('Jäätelöö Øre Võru Jérôme İstanbul ıçşğŁ');

SELECT ngrams('äöü', 1);

SELECT translateUTF8('Jäätelöö', 'äöü', 'aou');