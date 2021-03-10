CREATE TABLESPACE fastdata LOCATION '/fastdata';

BEGIN;

\set ON_ERROR_STOP on

CREATE LANGUAGE plpython3u;
CREATE EXTENSION rum;
CREATE EXTENSION hll;
CREATE EXTENSION pspacy;
CREATE EXTENSION pg_rollup;
CREATE EXTENSION pg_stat_statements;

/*******************************************************************************
 * generic helper functions
 */

/*
 * reverse an array, see: https://wiki.postgresql.org/wiki/Array_reverse
 */
CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray AS $$
SELECT ARRAY(
    SELECT $1[i]
    FROM generate_subscripts($1,1) AS s(i)
    ORDER BY i DESC
);
$$ LANGUAGE 'sql' STRICT IMMUTABLE PARALLEL SAFE;

/*
 * the btree index cannot support text column sizes that are large;
 * this function truncates the input to an acceptable size
 */
CREATE OR REPLACE FUNCTION btree_sanitize(t TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN SUBSTRING(t FOR 2048);
END
$$;

/*******************************************************************************
 * functions for extracting the components of a url stored as text
 * NOTE:
 * the extension pguri (https://github.com/petere/pguri) is specifically designed for storing url data;
 * but it requires that all input urls be properly formatted;
 * that will not be the case for our urls,
 * and so that's why we must manually implement these functions
 */

/*
 * remove the scheme from an input url
 *
 * FIXME: what to do for mailto:blah@gmail.com ?
 */
CREATE OR REPLACE FUNCTION url_remove_scheme(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN COALESCE(SUBSTRING(url, '[^:/]*//(.*)'),url);
END 
$$;

do $$
BEGIN
    assert( url_remove_scheme('https://cnn.com') = 'cnn.com');
    assert( url_remove_scheme('https://cnn.com/') = 'cnn.com/');
    assert( url_remove_scheme('https://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
    assert( url_remove_scheme('http://cnn.com') = 'cnn.com');
    assert( url_remove_scheme('http://cnn.com/') = 'cnn.com/');
    assert( url_remove_scheme('http://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
    assert( url_remove_scheme('cnn.com') = 'cnn.com');
    assert( url_remove_scheme('cnn.com/') = 'cnn.com/');
    assert( url_remove_scheme('www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_host(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_without_scheme TEXT = url_remove_scheme(url);
BEGIN
    RETURN SUBSTRING(url_without_scheme, '([^/?:]*):?[^/?]*[/?]?');
END 
$$;

do $$
BEGIN
    assert( url_host('https://cnn.com') = 'cnn.com');
    assert( url_host('https://cnn.com/') = 'cnn.com');
    assert( url_host('https://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com');
    assert( url_host('http://cnn.com') = 'cnn.com');
    assert( url_host('http://cnn.com/') = 'cnn.com');
    assert( url_host('http://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com');
    assert( url_host('cnn.com') = 'cnn.com');
    assert( url_host('cnn.com/') = 'cnn.com');
    assert( url_host('www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = 'www.cnn.com');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_path(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_without_scheme TEXT = url_remove_scheme(url);
BEGIN
    RETURN COALESCE(SUBSTRING(url_without_scheme, '[^/?]+([/][^;#?]*)'),'/');
END 
$$;

do $$
BEGIN
    assert( url_path('https://cnn.com') = '/');
    assert( url_path('https://cnn.com/') = '/');
    assert( url_path('https://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = '/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
    assert( url_path('http://cnn.com') = '/');
    assert( url_path('http://cnn.com/') = '/');
    assert( url_path('http://www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = '/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');
    assert( url_path('cnn.com') = '/');
    assert( url_path('cnn.com/') = '/');
    assert( url_path('www.cnn.com/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html') = '/2020/12/09/tech/facebook-antitrust-lawsuit-ftc-attorney-generals/index.html');

    assert( url_path('https://example.com/path/to/index.html?a=b&c=d') = '/path/to/index.html');
    assert( url_path('https://example.com/index.html?a=b&c=d') = '/index.html');
    assert( url_path('https://example.com/?a=b&c=d') = '/');

    assert( url_path('https://example.com/path/to/index.html;test?a=b&c=d') = '/path/to/index.html');
    assert( url_path('https://example.com/index.html;test?a=b&c=d') = '/index.html');
    assert( url_path('https://example.com/;test?a=b&c=d') = '/');

    assert( url_path('https://example.com/path/to/index.html#test') = '/path/to/index.html');
    assert( url_path('https://example.com/index.html#test') = '/index.html');
    assert( url_path('https://example.com/#test') = '/');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_query(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN COALESCE(SUBSTRING(url, '\?([^?#]*)'),'');
END 
$$;

do $$
BEGIN
    assert( url_query('https://example.com/path/to/index.html?a=b&c=d') = 'a=b&c=d');
    assert( url_query('https://example.com/index.html?a=b&c=d') = 'a=b&c=d');
    assert( url_query('https://example.com/?a=b&c=d') = 'a=b&c=d');

    assert( url_query('https://example.com/path/to/index.html?a=b&c=d#test') = 'a=b&c=d');
    assert( url_query('https://example.com/index.html?a=b&c=d#test') = 'a=b&c=d');
    assert( url_query('https://example.com/?a=b&c=d#test') = 'a=b&c=d');

    assert( url_query('https://example.com/path/to/index.html') = '');
    assert( url_query('https://example.com/index.html') = '');
    assert( url_query('https://example.com/') = '');

    assert( url_query('/path/to/index.html?a=b&c=d#test') = 'a=b&c=d');
    assert( url_query('/index.html?a=b&c=d#test') = 'a=b&c=d');
    assert( url_query('/?a=b&c=d#test') = 'a=b&c=d');
END;
$$ LANGUAGE plpgsql;


----------------------------------------
-- simplification functions

/*
 * remove extraneous leading subdomains from a host
 */
CREATE OR REPLACE FUNCTION host_simplify(host TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN COALESCE(
        SUBSTRING(host, '^www\d*\.(.*)'),
        SUBSTRING(host, '^m\.(.*)'),
        host
    );
END 
$$;

do $$
BEGIN
    assert( host_simplify('cnn.com') = 'cnn.com');
    assert( host_simplify('www.cnn.com') = 'cnn.com');
    assert( host_simplify('www2.cnn.com') = 'cnn.com');
    assert( host_simplify('www5.cnn.com') = 'cnn.com');
    assert( host_simplify('www577.cnn.com') = 'cnn.com');
    assert( host_simplify('bbc.co.uk') = 'bbc.co.uk');
    assert( host_simplify('www.bbc.co.uk') = 'bbc.co.uk');
    assert( host_simplify('en.wikipedia.org') = 'en.wikipedia.org');
    assert( host_simplify('m.wikipedia.org') = 'wikipedia.org');
    assert( host_simplify('naenara.com.kp') = 'naenara.com.kp');
END;
$$ LANGUAGE plpgsql;

/*
 * converts a host into the key syntax used by the common crawl
 * the main feature is that subdomains are in reverse order,
 * so string matches starting from the left hand side become increasingly specific
 */
CREATE OR REPLACE FUNCTION host_key(host TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN array_to_string(array_reverse(string_to_array(host,'.')),',')||')';
END 
$$;

do $$
BEGIN
    assert( host_key('cnn.com') = 'com,cnn)');
    assert( host_key('www.cnn.com') = 'com,cnn,www)');
    assert( host_key('www.bbc.co.uk') = 'uk,co,bbc,www)');
END;
$$ LANGUAGE plpgsql;

/*
 * converts from the host_key syntax into the standard host syntax;
 */
CREATE OR REPLACE FUNCTION host_unkey(host TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN array_to_string(array_reverse(string_to_array(substring(host from 0 for char_length(host)),',')),'.');
END 
$$;

do $$
BEGIN
    assert( host_unkey(host_key('cnn.com')) = 'cnn.com');
    assert( host_unkey(host_key('www.cnn.com')) = 'www.cnn.com');
    assert( host_unkey(host_key('www.bbc.co.uk')) = 'www.bbc.co.uk');
END;
$$ LANGUAGE plpgsql;

/*
 * removes default webpages like index.html from the end of the path,
 * and removes trailing slashes from the end of the path;
 * technically, these changes can modify the path to point to a new location,
 * but this is extremely rare in practice
 */
CREATE OR REPLACE FUNCTION path_simplify(path TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    path_without_index TEXT = COALESCE(
        SUBSTRING(path, '(.*/)index.\w{3,4}$'),
        path
    );
BEGIN
    RETURN COALESCE(
        SUBSTRING(path_without_index, '(.*)/$'),
        path_without_index
    );
END 
$$;

do $$
BEGIN
    assert( path_simplify('/path/to/index.html/more/paths') = '/path/to/index.html/more/paths');
    assert( path_simplify('/path/to/index.html') = '/path/to');
    assert( path_simplify('/path/to/index.htm') = '/path/to');
    assert( path_simplify('/path/to/index.asp') = '/path/to');
    assert( path_simplify('/path/to/') = '/path/to');
    assert( path_simplify('/index.html') = '');
    assert( path_simplify('/index.htm') = '');
    assert( path_simplify('/') = '');
    assert( path_simplify('') = '');
END;
$$ LANGUAGE plpgsql;


/*
 * sorts query terms and removes query terms used only for tracking
 * see: https://en.wikipedia.org/wiki/UTM_parameters
 * see: https://github.com/mpchadwick/tracking-query-params-registry/blob/master/data.csv
 * for the sorting step, see: https://stackoverflow.com/questions/2913368/sorting-array-elements
 */
CREATE OR REPLACE FUNCTION query_simplify(query TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN array_to_string(array(
        SELECT * FROM unnest(string_to_array(query,'&')) AS unnest
        WHERE unnest.unnest NOT LIKE 'utm_%'
        ORDER BY unnest.unnest ASC
    ),'&');
END 
$$;

do $$
BEGIN
    assert( query_simplify('a=1&b=2&utm_source=google.com') = 'a=1&b=2');
    assert( query_simplify('a=1&utm_source=google.com&b=2') = 'a=1&b=2');
    assert( query_simplify('utm_source=google.com&a=1&b=2') = 'a=1&b=2');
    assert( query_simplify('a=1&b=2') = 'a=1&b=2');
    assert( query_simplify('b=1&a=2') = 'a=2&b=1');
    assert( query_simplify('a=1') = 'a=1');
    assert( query_simplify('') = '');
END;
$$ LANGUAGE plpgsql;

----------------------------------------
-- functions for indexing

CREATE OR REPLACE FUNCTION url_host_key(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
BEGIN
    RETURN btree_sanitize(host_key(host_simplify(url_host(url_lower))));
END 
$$;

do $$
BEGIN
    assert( url_host_key('https://example.com') = 'com,example)');
    assert( url_host_key('https://example.com/') = 'com,example)');
    assert( url_host_key('https://example.com/#test') = 'com,example)');
    assert( url_host_key('https://example.com/?param=12') = 'com,example)');
    assert( url_host_key('https://example.com/path/to') = 'com,example)');
    assert( url_host_key('https://example.com/path/to/') = 'com,example)');
    assert( url_host_key('https://example.com/path/to/#test') = 'com,example)');
    assert( url_host_key('https://example.com/path/to/?param=12') = 'com,example)');
    assert( url_host_key('https://Example.com/Path/To/?Param=12') = 'com,example)');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_hostpath_key(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
BEGIN
    RETURN btree_sanitize(host_key(host_simplify(url_host(url_lower))) || path_simplify(url_path(url_lower)));
END 
$$;

do $$
BEGIN
    assert( url_hostpath_key('https://example.com') = 'com,example)');
    assert( url_hostpath_key('https://example.com/') = 'com,example)');
    assert( url_hostpath_key('https://example.com/#test') = 'com,example)');
    assert( url_hostpath_key('https://example.com/?param=12') = 'com,example)');
    assert( url_hostpath_key('https://example.com/path/to') = 'com,example)/path/to');
    assert( url_hostpath_key('https://example.com/path/to/') = 'com,example)/path/to');
    assert( url_hostpath_key('https://example.com/path/to/#test') = 'com,example)/path/to');
    assert( url_hostpath_key('https://example.com/path/to/?param=12') = 'com,example)/path/to');
    assert( url_hostpath_key('https://Example.com/Path/To/?Param=12') = 'com,example)/path/to');
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION url_hostpathquery_key(url TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    url_lower TEXT = lower(url);
    query TEXT = query_simplify(url_query(url_lower));
BEGIN
    RETURN btree_sanitize(
        host_key(host_simplify(url_host(url_lower))) || 
        path_simplify(url_path(url_lower)) || 
        CASE WHEN length(query)>0
            THEN '?' || query
            ELSE ''
        END
    );
END 
$$;

do $$
BEGIN
    assert( url_hostpathquery_key('https://example.com') = 'com,example)');
    assert( url_hostpathquery_key('https://example.com/') = 'com,example)');
    assert( url_hostpathquery_key('https://example.com/#test') = 'com,example)');
    assert( url_hostpathquery_key('https://example.com/?param=12') = 'com,example)?param=12');
    assert( url_hostpathquery_key('https://example.com/path/to') = 'com,example)/path/to');
    assert( url_hostpathquery_key('https://example.com/path/to/') = 'com,example)/path/to');
    assert( url_hostpathquery_key('https://example.com/path/to/#test') = 'com,example)/path/to');
    assert( url_hostpathquery_key('https://example.com/path/to/?param=12') = 'com,example)/path/to?param=12');
    assert( url_hostpathquery_key('https://Example.com/Path/To/?Param=12') = 'com,example)/path/to?param=12');
END;
$$ LANGUAGE plpgsql;


/*******************************************************************************
 * preloaded data
 ******************************************************************************/

/*
 * stores the information about metahtml's test cases
 */
CREATE TABLE metahtml_test (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    jsonb JSONB NOT NULL
);
COPY metahtml_test(jsonb) FROM '/tmp/metahtml/golden.jsonl';

SELECT create_rollup(
    'metahtml_test',
    'metahtml_test_rollup',
    rollups => $$
        hll(jsonb->>'url') AS url,
        hll(url_hostpathquery_key(jsonb->>'url')) AS hostpathquery,
        hll(url_hostpath_key(jsonb->>'url')) AS hostpath,
        hll(url_host_key(jsonb->>'url')) AS host
    $$
);

SELECT create_rollup(
    'metahtml_test',
    'metahtml_test_rollup_host',
    wheres => $$
        url_host_key(jsonb->>'url') AS host
    $$,
    rollups => $$
        hll(jsonb->>'url') AS url,
        hll(url_hostpathquery_key(jsonb->>'url')) AS hostpathquery,
        hll(url_hostpath_key(jsonb->>'url')) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml_test',
    'metahtml_test_language',
    wheres => $$
        jsonb->>'language' AS language
    $$,
    rollups => $$
        hll(jsonb->>'url') AS url,
        hll(url_hostpathquery_key(jsonb->>'url')) AS hostpathquery,
        hll(url_hostpath_key(jsonb->>'url')) AS hostpath,
        hll(url_host_key(jsonb->>'url')) AS host
    $$
);

SELECT create_rollup(
    'metahtml_test',
    'metahtml_test_language2',
    wheres => $$
        substring(jsonb->>'language' from 1 for 2) AS language_iso2
    $$,
    rollups => $$
        hll(jsonb->>'url') AS url,
        hll(url_hostpathquery_key(jsonb->>'url')) AS hostpathquery,
        hll(url_hostpath_key(jsonb->>'url')) AS hostpath,
        hll(url_host_key(jsonb->>'url')) AS host
    $$
);

/*
 * stores manually annotated information about hostnames
 */
CREATE TABLE hostnames (
    id_hostnames INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hostname VARCHAR(253) NOT NULL CHECK (hostname = lower(hostname)),
    priority TEXT,
    name_native TEXT,
    name_latin TEXT,
    language TEXT,
    country TEXT,
    type TEXT
);
COPY hostnames(hostname,priority,name_native,name_latin,country,language,type) FROM '/tmp/data/hostnames.csv' DELIMITER ',' CSV HEADER;

CREATE VIEW hostnames_untested AS (
    SELECT hostname,country,language
    FROM hostnames
    WHERE
        COALESCE(priority,'') != 'ban' AND
        url_host_key(hostnames.hostname) NOT IN (
            SELECT DISTINCT url_host_key(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY country,hostname
    );

/*
 * uses data scraped from allsides.com
 */
CREATE TABLE allsides (
    id_allsides INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT,
    type TEXT,
    name TEXT,
    bias TEXT
);
COPY allsides(url,type,name,bias) FROM '/tmp/data/allsides/allsides.csv' CSV HEADER;

CREATE VIEW allsides_untested AS (
    SELECT DISTINCT
        url_host_key(url) AS host_key
    FROM allsides
    WHERE
        url_host_key(url) NOT IN (
            SELECT DISTINCT url_host_key(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_key
    );

SELECT create_rollup(
    'allsides',
    'allsides_rollup_type',
    wheres => $$
        type
    $$
);

SELECT create_rollup(
    'allsides',
    'allsides_rollup_bias',
    wheres => $$
        bias
    $$
);

SELECT create_rollup(
    'allsides',
    'allsides_rollup_typebias',
    wheres => $$
        type,
        bias
    $$
);

/*
 * uses data scraped from mediabiasfactcheck.com
 */
CREATE TABLE mediabiasfactcheck (
    id_allsides INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT,
    name TEXT,
    image_pseudoscience TEXT,
    image_factual TEXT,
    image_conspiracy TEXT,
    image_bias TEXT,
    freedom_rank TEXT,
    country TEXT
);
COPY mediabiasfactcheck(url,name,image_pseudoscience,image_factual,image_conspiracy,image_bias,freedom_rank,country) FROM '/tmp/data/mediabiasfactcheck/mediabiasfactcheck.csv' CSV HEADER;

CREATE VIEW mediabiasfactcheck_untested AS (
    SELECT DISTINCT
        url_host_key(url) AS host_key
    FROM mediabiasfactcheck
    WHERE
        url_host_key(url) NOT IN (
            SELECT DISTINCT url_host_key(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_key
    );

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_image_bias',
    wheres => $$
        image_bias
    $$
);

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_image_conspiracy',
    wheres => $$
        image_conspiracy
    $$
);

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_image_pseudoscience',
    wheres => $$
        image_pseudoscience
    $$
);

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_image_factual',
    wheres => $$
        image_factual
    $$
);

SELECT create_rollup(
    'mediabiasfactcheck',
    'mediabiasfactcheck_rollup_country',
    wheres => $$
        country
    $$
);

/*
 * This dataset annotates the bias of specific urls
 * See: https://deepblue.lib.umich.edu/data/concern/data_sets/8w32r569d?locale=en
 */
CREATE TABLE quantifyingnewsmediabias (
    id_quantifyingnewsmediabias INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT,
    q3 TEXT,
    perceived SMALLINT,
    primary_topic TEXT,
    secondary_topic TEXT,
    democrat_vote TEXT,
    republican_vote TEXT
);
COPY quantifyingnewsmediabias(url,q3,perceived,primary_topic,secondary_topic,democrat_vote,republican_vote) FROM '/tmp/data/QuantifyingNewsMediaBias/newsArticlesWithLabels.tsv' DELIMITER E'\t' CSV HEADER;

CREATE VIEW quantifyingnewsmediabias_untested AS (
    SELECT DISTINCT
        url_host_key(url) AS host_key
    FROM quantifyingnewsmediabias
    WHERE
        url_host_key(url) NOT IN (
            SELECT DISTINCT url_host_key(jsonb->>'url')
            FROM metahtml_test
        )
    ORDER BY host_key
    );

SELECT create_rollup(
    'quantifyingnewsmediabias',
    'quantifyingnewsmediabias_rollup_host',
    wheres => $$
        url_host_key(url) AS host
    $$
);

SELECT create_rollup(
    'quantifyingnewsmediabias',
    'quantifyingnewsmediabias_rollup',
    rollups => $$
        hll(url) AS url,
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath,
        hll(url_host_key(url)) AS host
    $$
);

/*******************************************************************************
 * main tables
 ******************************************************************************/

/*
 * stores information about the source of the data
 */
CREATE TABLE source (
    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    name TEXT UNIQUE NOT NULL
);
INSERT INTO source (id,name) VALUES (-1,'metahtml');

/*
 * The primary table for storing extracted content
 */

CREATE TABLE metahtml (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_source INTEGER NOT NULL REFERENCES source(id),
    accessed_at TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    url TEXT NOT NULL, -- FIXME: add this constraint? CHECK (uri_normalize(uri(url)) = uri(url)),
    jsonb JSONB NOT NULL,
    title tsvector,
    content tsvector
);

-- rollups for tracking debug info for the metahtml library

/*
SELECT create_rollup (
    'metahtml',
    'metahtml_versions',
    wheres => $$
        jsonb->'version' AS version
    $$,
    rollups => $$
        hll(url) as url,
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath,
        hll(url_host_key(url)) AS host
    $$
);

-- FIXME:
-- the "type" column is not detailed enough, but str(e) is too detailed.
SELECT create_rollup (
    'metahtml',
    'metahtml_exceptions',
    wheres => $$
        jsonb->'exception'->>'type' AS type,
        jsonb->'exception'->>'location' AS location,
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

SELECT create_rollup (
    'metahtml',
    'metahtml_exceptions_host',
    wheres => $$
        url_host(url) AS host,
        jsonb->'exception'->>'type' AS type,
        jsonb->'exception'->>'location' AS location,
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

-- rollup tables for measuring links/pagerank

-- FIXME:
-- add an index for finding backlinks
--FIXME:
--comments not allowed in wheres/distincts lists
--
--FIXME: these should be in the distincts
--jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href' AS dest_url,
--url_hostpathquery_key(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest_hostpathquery,
--url_hostpath_key(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest_hostpath

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_linksall_host',
    wheres => $$
        url_host(url) AS src,
        url_host(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest,
    $$,
    rollups => $$
        hll(url) AS src_url,
        hll(url_hostpathquery_key(url)) AS src_hostpathquery,
        hll(url_hostpath_key(url)) AS src_hostpath
    $$
);
*/

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_linkscontent_host',
    wheres => $$
        url_host(url) AS src,
        url_host(jsonb_array_elements(jsonb->'links.content'->'best'->'value')->>'href') AS dest,
    $$,
    rollups => $$
        hll(url) AS src_url,
        hll(url_hostpathquery_key(url)) AS src_hostpathquery,
        hll(url_hostpath_key(url)) AS src_hostpath
    $$
);
*/

/*
-- FIXME:
-- we shsould add filtering onto this so that we only record exact pagerank details for a small subset of links
-- FIXME:
-- does this create too many locks?
SELECT create_rollup(
    'metahtml',
    'metahtml_linksall_hostpath',
    wheres => $$
        url_hostpath_key(url) AS src,
        url_hostpath_key(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest,
    $$
);
*/

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_linkscontent_hostpath',
    wheres => $$
        url_hostpath_key(url) AS src,
        url_hostpath_key(jsonb_array_elements(jsonb->'links.content'->'best'->'value')->>'href') AS dest,
    $$
);
*/

-- rollups for text

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_texthostmonth',
    tablespace => 'fastdata',
    wheres => $$
        unnest(tsvector_to_array(title || content)) AS alltext,
        url_host(url) AS host,
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_textmonth',
    tablespace => 'fastdata',
    wheres => $$
        unnest(tsvector_to_array(title || content)) AS alltext,
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_textlangmonth',
    tablespace => 'fastdata',
    wheres => $$
        unnest(tsvector_to_array(title || content)) AS alltext,
        jsonb->'language'->'best'->>'value' AS language, 
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_langmonth',
    tablespace => 'fastdata',
    wheres => $$
        jsonb->'language'->'best'->>'value' AS language, 
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

-- other rollups

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_langhost',
    wheres => $$
        url_host(url) AS host,
        jsonb->'language'->'best'->>'value' AS language
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_lang',
    wheres => $$
        jsonb->'language'->'best'->>'value' AS language
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_source',
    wheres => $$
        id_source
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath,
        hll(url_host_key(url)) AS host
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_type',
    wheres => $$
        jsonb->'type'->'best'->>'value' AS type
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_hosttype',
    wheres => $$
        url_host(url) AS host,
        jsonb->'type'->'best'->>'value' AS type
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_host',
    wheres => $$
        url_host(url) AS host
    $$,
    rollups => $$
        hll(id_source),
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_hostaccess',
    wheres => $$
        url_host(url) AS host_key,
        date_trunc('day', accessed_at) AS access_day
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_insert',
    wheres => $$
        date_trunc('hour', inserted_at) AS insert_hour
    $$,
    rollups => $$
        hll(id_source),
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath,
        hll(url_host_key(url)) AS host
    $$
);

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_hostinsert',
    wheres => $$
        url_host(url) AS host_key,
        date_trunc('hour', inserted_at) AS insert_hour
    $$,
    rollups => $$
        hll(id_source),
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_access',
    wheres => $$
        date_trunc('day', accessed_at) AS access_day
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_hostmonth',
    wheres => $$
        url_host(url) AS host,
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_month',
    tablespace => 'fastdata',
    wheres => $$
        date_trunc('month',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_pub',
    wheres => $$
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_accesspub',
    wheres => $$
        date_trunc('day', accessed_at) AS access_day,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    rollups => $$
        hll(url),
        hll(url_hostpathquery_key(url)) AS hostpathquery,
        hll(url_hostpath_key(url)) AS hostpath
    $$
);
*/

/* indexes for text search of the form

SELECT
    jsonb->'title'->'best'->>'value'
FROM metahtml
WHERE
    spacy_tsvector(
        jsonb->'language'->'best'->>'value',
        jsonb->'title'->'best'->>'value'
    ) @@ 
    spacy_tsquery('en', 'covid');
*/

CREATE INDEX metahtml_title_rumidx ON metahtml USING rum (title) TABLESPACE fastdata;
CREATE INDEX metahtml_content_rumidx ON metahtml USING rum (content) TABLESPACE fastdata;
/*
CREATE INDEX metahtml_title_rumidx ON metahtml USING rum (
    spacy_tsvector(
        jsonb->'language'->'best'->>'value',
        jsonb->'title'->'best'->>'value'
    ));
CREATE INDEX metahtml_content_rumidx ON metahtml USING rum (
    spacy_tsvector(
        jsonb->'language'->'best'->>'value',
        jsonb->'content'->'best'->>'value'
    ));

CREATE INDEX metahtml_hosttitle_rumidx ON metahtml USING rum (
    url_host_key(url),
    spacy_tsvector(
        jsonb->'language'->'best'->>'value',
        jsonb->'title'->'best'->>'value'
    ));
CREATE INDEX metahtml_hostcontent_rumidx ON metahtml USING rum (
    url_host_key(url),
    spacy_tsvector(
        jsonb->'language'->'best'->>'value',
        jsonb->'content'->'best'->>'value'
    ));
*/

COMMIT;
