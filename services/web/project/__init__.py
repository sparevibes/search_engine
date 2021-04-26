# imports
import os
import time
import sqlalchemy
import pspacy
from sqlalchemy.sql import text
from flask import Flask, jsonify, send_from_directory, render_template, g, request
from flask_sqlalchemy import SQLAlchemy

# creates the flask app
app = Flask(__name__)
app.config.from_object('project.config.Config')


def dict2html(d):
    html='<table>'
    for k in d:
        html+=f'<tr><td>{k}</td><td>{d[k]}</td></tr>'
    html+='</table>'
    return html


def res2html(res,col_formatter=None,transpose=False,click_headers=False):
    rows=[list(res.keys())]+list(res)
    if transpose:
        rows=list(map(list, zip(*rows)))
    html='<table>'
    for i,row in enumerate(rows):
        html+='<tr>'
        if i==0 and not transpose:
            td='th'
            html+=f'<{td}></{td}>'
        else:
            td='td'
            html+=f'<td>{i}</td>'
        for j,col in enumerate(row):
            val = None
            try:
                val = col_formatter(res.keys()[j],col,i==0)
            except:
                if i>0 and col_formatter is not None:
                    val = col_formatter(res.keys()[j],col)
            if val is None:
                val = col
            if type(col) == int or type(col) == float:
                td_class='numeric'
            else:
                td_class='text'
            html+=f'<{td} class={td_class}>{val}</td>'
        html+='</tr>'
    html+='</table>'
    return html

################################################################################
# routes
################################################################################


@app.route('/')
def index():
    return render_template(
        'index.html'
        )


@app.route('/metahtml')
def metahtml():
    id = request.args.get('id')
    if id is None:
        return render_template(
            'metahtml',
            )
    else:
        sql=text(f'''
        SELECT 
            accessed_at,
            inserted_at,
            url,
            jsonb
        FROM metahtml
        WHERE id=:id
        ''')
        res = g.connection.execute(sql,{
            'id':id
            }).first()

        jsonb = {}
        for key in ['author','timestamp.published','timestamp.modified','url.canonical','language','version']:
            try:
                value = res['jsonb'][key]['best']['value']
            except (TypeError,KeyError):
            #except KeyError:
                value = ''
            jsonb[key] = value
        jsonb_html = dict2html(jsonb)
        try:
            title = res['jsonb']['title']['best']['value']
            content = res['jsonb']['content']['best']['value']['html']
        except KeyError:
            title = None
            content = None

        return render_template(
            'metahtml.html',
            title = title,
            content = content,
            jsonb_html = jsonb_html
            )


@app.route('/ngrams')
def ngrams():

    query = request.args.get('query')
    if query is None:
        return index()

    ts_query = pspacy.lemmatize_query('en', query)

    terms = [ term for term in ts_query.split() if term != '&' ]

    if len(terms)<1:
        return render_template(
            'fullsearch.html',
            )

    sql=text(f'''
    select  
        extract(epoch from x.time ) as x,
        '''+
        ''',
        '''.join([f'''
        coalesce(y{i}/total.total,0) as y{i}
        ''' for i,term in enumerate(terms) ])
        +'''
    from (
        select generate_series('2000-01-01', '2020-12-31', '1 month'::interval) as time
    ) as x
    left outer join (
        select
            hostpath as total,
            timestamp_published as time
        from metahtml_rollup_langmonth
        where 
                language = 'en'
            and timestamp_published >= '2000-01-01 00:00:00' 
            and timestamp_published <= '2020-12-31 23:59:59'
    ) total on total.time=x.time
    '''
    +'''
    '''.join([f'''
    left outer join (
        select
            hostpath as y{i},
            timestamp_published as time
        from metahtml_rollup_textlangmonth
        where 
            alltext = :term{i}
            and language = 'en'
            and timestamp_published >= '2000-01-01 00:00:00' 
            and timestamp_published <= '2020-12-31 23:59:59'
    ) y{i} on x.time=y{i}.time
    ''' for i,term in enumerate(terms) ])
    +
    '''
    order by x asc;
    ''')
    res = list(g.connection.execute(sql,{
        f'term{i}':term
        for i,term in enumerate(terms)
        }))
    x = [ row.x for row in res ]
    ys = [ [ row[i+1] for row in res ] for i,term in enumerate(terms) ] 
    colors = ['red','green','blue','black','purple','orange','pink','aqua']


    sql=text(f'''
    SELECT 
        id,
        jsonb->'title'->'best'->>'value' AS title,
        jsonb->'description'->'best'->>'value' AS description
    FROM metahtml
    WHERE
        to_tsquery('simple', :ts_query) @@ content AND
        jsonb->'type'->'best'->>'value' = 'article'
    OFFSET 0
    LIMIT 10
    ''')
    res=g.connection.execute(sql,{
        'ts_query':ts_query
        })
    return render_template(
        'fullsearch.html',
        query=query,
        results=res,
        x = x,
        ys = ys,
        terms = zip(terms,colors) ,
        )


@app.route("/static/<path:filename>")
def staticfiles(filename):
    return send_from_directory(app.config["STATIC_FOLDER"], filename)


################################################################################
# the code below creates a db connection and disconnects for each request;
# it replaces every occurrence of the string __EXECUTION_TIME__
# with the actual time to generate the webpage;
# this could result in some rendering bugs on some webpages
# see: https://stackoverflow.com/questions/12273889/calculate-execution-time-for-every-page-in-pythons-flask
################################################################################

engine = sqlalchemy.create_engine(app.config['DB_URI'], connect_args={
    'connect_timeout': 10,
    'application_name': 'novichenko/web',
    })


@app.before_request
def before_request():
    g.start = time.time()
    g.connection = engine.connect()


@app.after_request
def after_request(response):
    diff = time.time() - g.start
    diff_str = f'{"%0.3f"%diff} seconds'
    if ((response.response) and
        (200 <= response.status_code < 300) and
        (response.content_type.startswith('text/html'))):
        response.set_data(response.get_data().replace(
            b'__EXECUTION_TIME__', bytes(diff_str, 'utf-8')))
    return response


@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'connection'):
        g.connection.close()
