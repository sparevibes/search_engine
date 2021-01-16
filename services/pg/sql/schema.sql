BEGIN;

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS rum;
CREATE EXTENSION IF NOT EXISTS hll;
CREATE EXTENSION IF NOT EXISTS pspacy;
CREATE EXTENSION IF NOT EXISTS pg_rollup;

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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN SUBSTRING(t FOR 2048);
END
$$;

/*
 * converts a string into a truncated UTC timestamp
 * FIXME:
 * should we delete this?
 */
CREATE OR REPLACE FUNCTION simplify_timestamp(field TEXT, t TEXT)
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
AS $$
BEGIN
    RETURN date_trunc(field,t::timestamptz AT TIME ZONE 'UTC');
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
RETURNS TEXT language plpgsql IMMUTABLE STRICT PARALLEL SAFE
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
 * data tables
 ******************************************************************************/

-- FIXME:
-- SERIAL -> INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY

/*
 * stores manually annotated information about hostnames
 */
CREATE TEMPORARY TABLE hostnames (
    id_hostnames SERIAL PRIMARY KEY,
    hostname VARCHAR(253) NOT NULL CHECK (hostname = lower(hostname)),
    priority TEXT,
    name_native TEXT,
    name_latin TEXT,
    lang TEXT,
    country TEXT,
    type TEXT
);

/*
 *
*/
CREATE TABLE source (
    id SERIAL PRIMARY KEY,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    name TEXT UNIQUE NOT NULL
);
INSERT INTO source (id,name) VALUES (-1,'metahtml');

/*
 * The primary table for storing extracted content
 */

CREATE TABLE metahtml (
    id BIGSERIAL PRIMARY KEY,
    id_source INTEGER NOT NULL REFERENCES source(id),
    accessed_at TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    url TEXT NOT NULL, -- FIXME: add this constraint? CHECK (uri_normalize(uri(url)) = uri(url)),
    jsonb JSONB NOT NULL,
    title tsvector,
    content tsvector
);

-- rollups for tracking debug info for the metahtml library

SELECT create_rollup (
    'metahtml',
    'metahtml_versions',
    wheres => $$
        jsonb->'version' AS version
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath,
        url_host_key(url) AS host
    $$
);

-- FIXME:
-- the "type" column is not detailed enough, but str(e) is too detailed.
/*
SELECT create_rollup (
    'metahtml',
    'metahtml_exceptions',
    wheres => $$
        jsonb->'exception'->>'type' AS type,
        jsonb->'exception'->>'location' AS location,
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);
*/

SELECT create_rollup (
    'metahtml',
    'metahtml_exceptions_host',
    wheres => $$
        url_host(url) AS host,
        jsonb->'exception'->>'type' AS type,
        jsonb->'exception'->>'location' AS location,
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

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

SELECT create_rollup(
    'metahtml',
    'metahtml_linksall_host',
    wheres => $$
        url_host(url) AS src,
        url_host(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest,
    $$,
    distincts => $$
        url AS src_url,
        url_hostpathquery_key(url) AS src_hostpathquery,
        url_hostpath_key(url) AS src_hostpath
    $$
);

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_linkscontent_host',
    wheres => $$
        url_host(url) AS src,
        url_host(jsonb_array_elements(jsonb->'links.content'->'best'->'value')->>'href') AS dest,
    $$,
    distincts => $$
        url AS src_url,
        url_hostpathquery_key(url) AS src_hostpathquery,
        url_hostpath_key(url) AS src_hostpath
    $$
);

-- FIXME:
-- we shsould add filtering onto this so that we only record exact pagerank details for a small subset of links
SELECT create_rollup(
    'metahtml',
    'metahtml_linksall_hostpath',
    wheres => $$
        url_hostpath_key(url) AS src,
        url_hostpath_key(jsonb_array_elements(jsonb->'links.all'->'best'->'value')->>'href') AS dest,
    $$
);

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

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_texthostpub',
    wheres => $$
        unnest(tsvector_to_array(title || content)) AS alltext,
        url_host(url) AS host,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_titlehostpub',
    wheres => $$
        btree_sanitize(unnest(tsvector_to_array(spacy_tsvector(
            jsonb->'language'->'best'->>'value',
            jsonb->'title'->'best'->>'value'
            )))) AS title,
        url_host(url) AS host,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_contenthostpub',
    wheres => $$
        btree_sanitize(unnest(tsvector_to_array(spacy_tsvector(
            jsonb->'language'->'best'->>'value',
            jsonb->'content'->'best'->>'value'
            )))) AS content,
        url_host(url) AS host,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);
*/

-- other rollups

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_source',
    wheres => $$
        id_source
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath,
        url_host_key(url) AS host
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_type',
    wheres => $$
        jsonb->'type'->'best'->>'value' AS type
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);
*/

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_hosttype',
    wheres => $$
        url_host(url) AS host,
        jsonb->'type'->'best'->>'value' AS type
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_host',
    wheres => $$
        url_host(url) AS host
    $$,
    distincts => $$
        id_source,
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);
*/

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_hostaccess',
    wheres => $$
        url_host(url) AS host_key,
        date_trunc('day', accessed_at) AS access_day
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_insert',
    wheres => $$
        date_trunc('hour', inserted_at) AS insert_hour
    $$,
    distincts => $$
        id_source,
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath,
        url_host_key(url) AS host
    $$
);

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_hostinsert',
    wheres => $$
        url_host(url) AS host_key,
        date_trunc('hour', inserted_at) AS insert_hour
    $$,
    distincts => $$
        id_source,
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_access',
    wheres => $$
        date_trunc('day', accessed_at) AS access_day
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);
*/

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_hostpub',
    wheres => $$
        url_host(url) AS host,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

/*
SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_pub',
    wheres => $$
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);
*/

SELECT create_rollup(
    'metahtml',
    'metahtml_rollup_accesspub',
    wheres => $$
        date_trunc('day', accessed_at) AS access_day,
        date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published
    $$,
    distincts => $$
        url,
        url_hostpathquery_key(url) AS hostpathquery,
        url_hostpath_key(url) AS hostpath
    $$
);

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

CREATE INDEX metahtml_title_rumidx ON metahtml USING rum (title);
CREATE INDEX metahtml_content_rumidx ON metahtml USING rum (content);
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
