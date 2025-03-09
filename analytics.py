import asyncio
import asyncpg
import duckdb
from os import getenv
import pandas as pd
import streamlit as st

async def fetch_extract():
    user = getenv("PGUSER")
    password = getenv("PGPASSWORD")
    host = getenv("PGHOST")
    port = getenv("PORT")
    database = getenv("PGDATABASE")
    cloud = await asyncpg.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    df = pd.DataFrame(await cloud.fetch(
        """
            SELECT 
                r.id, 
                r.date, 
                c.category, 
                p.first_name, 
                p.last_name, 
                ro.role, 
                a.activity, 
                r.hours, 
                r.money, 
                r.comments, 
                r.category_id,
                r.name_id,
                r.role_id,
                r.activity_id
            FROM records r
            JOIN categories c ON c.id = r.category_id
            JOIN people p ON p.id = r.name_id
            JOIN roles ro ON ro.id = r.role_id
            JOIN activities a ON a.id = r.activity_id
            ORDER BY r.date
        """
    ), columns=['id', 'date', 'category', 'first_name', 'last_name', 'role', 'activity', 'hours', 'money', 'comments', 'category_id', 'name_id', 'role_id', 'activity_id'])

    await cloud.close()

    return df

async def fetch_people():
    user = getenv("PGUSER")
    password = getenv("PGPASSWORD")
    host = getenv("PGHOST")
    port = getenv("PORT")
    database = getenv("PGDATABASE")
    cloud = await asyncpg.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    df = pd.DataFrame(await cloud.fetch('SELECT * FROM people'), columns=['id', 'first_name', 'last_name', 'dob', 'rate'])

    await cloud.close()

    return df

extract = asyncio.run(fetch_extract())

people = asyncio.run(fetch_people())

lou = duckdb.sql("""SELECT * FROM extract
                 WHERE first_name = 'Lou' AND category = 'Specific Assistance' AND activity LIKE 'Hassan%'""").df()

hassan = duckdb.sql("""SELECT * FROM extract
                    WHERE first_name = 'Hassan'""").df()

lou_to_hassan = duckdb.sql("""SELECT first_name||' '||last_name AS Name, SUM(hours) AS 'Total Hours' FROM extract
                           WHERE category = 'Specific Assistance' AND activity LIKE 'Hassan%' AND YEAR(date) >= 2021
                           GROUP BY first_name, last_name""").df()

hassan_hours = duckdb.sql("""SELECT first_name||' '||last_name AS Name, SUM(hours) AS 'Total Hours' FROM extract
                           WHERE first_name = 'Hassan' AND YEAR(date) >= 2021
                           GROUP BY first_name, last_name""").df()

cat_1800_cash = duckdb.sql("""SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                          SUM(e.hours) AS Hours,
                          p.rate AS Rate,
                          p.rate * SUM(e.hours) AS Pay
                          FROM extract e
                          JOIN people p ON p.id = e.name_id
                          WHERE e.category = '1800 Lodi'
                          AND e.first_name IN ('Cowboy','Day Labor','Hassan','James','Jeff','Joe','Mo','Mupenzi','Ray','Rich','Sa','Samatay','Scott','Shamuel','Tat','Tom','Vince','Walt')
                          AND e.last_name IN ('Adams','Dwyer','Greco','Netto','Joe','Irakiza','K','It','King','Tun','Reynolds','Lazzaro','Smith','none')
                          GROUP BY e.first_name, e.last_name, p.rate
                          ORDER BY e.first_name""").df()

cat_1800_cash_total = duckdb.sql("""WITH cat_1800_cash AS (
                                SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                                SUM(e.hours) AS Hours,
                                p.rate AS Rate,
                                p.rate * SUM(e.hours) AS Pay
                                FROM extract e
                                JOIN people p ON p.id = e.name_id
                                WHERE e.category = '1800 Lodi'
                                AND e.first_name IN ('Cowboy','Day Labor','Hassan','James','Jeff','Joe','Mo','Mupenzi','Ray','Rich','Sa','Samatay','Scott','Shamuel','Tat','Tom','Vince','Walt')
                                AND e.last_name IN ('Adams','Dwyer','Greco','Netto','Joe','Irakiza','K','It','King','Tun','Reynolds','Lazzaro','Smith','none')
                                GROUP BY e.first_name, e.last_name, p.rate
                                ORDER BY e.first_name)
                                SELECT SUM(Hours) AS 'TOTAL HOURS', SUM(Pay) AS 'TOTAL EQUIVALENT CASH' FROM cat_1800_cash""").df()

cat_1800_cny = duckdb.sql("""SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                         SUM(e.hours) AS Hours,
                         p.rate AS Rate,
                         p.rate * SUM(e.hours) AS Pay
                         FROM extract e
                         JOIN people p ON p.id = e.name_id
                         WHERE e.category = '1800 Lodi'
                         AND e.first_name IN ('Corey', 'Jacob', 'Kawreban', 'Kazen', 'Natori', 'Neil', 'Randall', 'Todd', 'Yasin')
                         AND e.last_name IN ('Farrow', 'Trotman', 'none')
                         GROUP BY e.first_name, e.last_name, p.rate
                         ORDER BY e.first_name""").df()

cat_1800_cny_total = duckdb.sql("""WITH cat_1800_cny AS (
                                SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                                SUM(e.hours) AS Hours,
                                p.rate AS Rate,
                                p.rate * SUM(e.hours) AS Pay
                                FROM extract e
                                JOIN people p ON p.id = e.name_id
                                WHERE e.category = '1800 Lodi'
                                AND e.first_name IN ('Corey', 'Jacob', 'Kawreban', 'Kazen', 'Natori', 'Neil', 'Randall', 'Todd', 'Yasin')
                                AND e.last_name IN ('Farrow', 'Trotman', 'none')
                                GROUP BY e.first_name, e.last_name, p.rate
                                ORDER BY e.first_name)
                                SELECT SUM(Hours) AS 'TOTAL HOURS', SUM(Pay) AS 'TOTAL EQUIVALENT CASH' FROM cat_1800_cny""").df()

cat_1800_vol = duckdb.sql("""SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                         SUM(e.hours) AS Hours,
                         p.rate AS Rate,
                         p.rate * SUM(e.hours) AS Pay
                         FROM extract e
                         JOIN people p ON p.id = e.name_id
                         WHERE e.category = '1800 Lodi'
                         AND e.first_name NOT IN ('Corey', 'Jacob', 'Kawreban', 'Kazen', 'Natori', 'Neil', 'Randall', 'Todd', 'Yasin', 'Cowboy','Day Labor','Hassan','James','Jeff','Joe','Mo',
                         'Mupenzi','Ray','Rich','Sa','Samatay','Scott','Shamuel','Tat','Tom','Vince','Walt','El','Marvin')
                         AND e.last_name NOT IN ('Farrow', 'Trotman', 'Adams','Dwyer','Greco','Netto','Joe','Irakiza','K','It','King','Tun','Reynolds','Lazzaro','Smith')
                         GROUP BY e.first_name, e.last_name, p.rate
                         ORDER BY e.first_name""").df()

cat_1800_vol_total = duckdb.sql("""WITH cat_1800_vol AS (
                                SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                                SUM(e.hours) AS Hours,
                                p.rate AS Rate,
                                p.rate * SUM(e.hours) AS Pay
                                FROM extract e
                                JOIN people p ON p.id = e.name_id
                                WHERE e.category = '1800 Lodi'
                                AND e.first_name NOT IN ('Corey', 'Jacob', 'Kawreban', 'Kazen', 'Natori', 'Neil', 'Randall', 'Todd', 'Yasin', 'Cowboy','Day Labor','Hassan','James','Jeff','Joe','Mo',
                                'Mupenzi','Ray','Rich','Sa','Samatay','Scott','Shamuel','Tat','Tom','Vince','Walt','El','Marvin')
                                AND e.last_name NOT IN ('Farrow', 'Trotman', 'Adams','Dwyer','Greco','Netto','Joe','Irakiza','K','It','King','Tun','Reynolds','Lazzaro','Smith')
                                GROUP BY e.first_name, e.last_name, p.rate
                                ORDER BY e.first_name)
                                SELECT SUM(Hours) AS 'TOTAL HOURS', SUM(Pay) AS 'TOTAL EQUIVALENT CASH' FROM cat_1800_vol""").df()

cat_1809_cash = duckdb.sql("""SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                          SUM(e.hours) AS Hours,
                          p.rate AS Rate,
                          p.rate * SUM(e.hours) AS Pay
                          FROM extract e
                          JOIN people p ON p.id = e.name_id
                          WHERE e.category = '1809 Lodi'
                          AND e.first_name IN ('Antonio', 'Mo', 'Sa')
                          AND e.last_name IN ('Joe', 'It', 'none')
                          GROUP BY e.first_name, e.last_name, p.rate
                          ORDER BY e.first_name""").df()

cat_1809_cash_total = duckdb.sql("""WITH cat_1809_cash AS (
                                SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                                SUM(e.hours) AS Hours,
                                p.rate AS Rate,
                                p.rate * SUM(e.hours) AS Pay
                                FROM extract e
                                JOIN people p ON p.id = e.name_id
                                WHERE e.category = '1809 Lodi'
                                AND e.first_name IN ('Antonio', 'Mo', 'Sa')
                                AND e.last_name IN ('Joe', 'It', 'none')
                                GROUP BY e.first_name, e.last_name, p.rate
                                ORDER BY e.first_name)
                                SELECT SUM(Hours) AS 'TOTAL HOURS', SUM(Pay) AS 'TOTAL EQUIVALENT CASH' FROM cat_1809_cash""").df()

fw_1809 = duckdb.sql("""SELECT e.first_name||' '||e.last_name AS Name,
                    SUM(e.hours) AS Hours,
                    p.rate AS Rate,
                    p.rate * SUM(e.hours) AS Pay
                    FROM extract e
                    JOIN people p ON p.id = e.name_id
                    WHERE e.category = '1809 Lodi'
                    AND e.first_name = 'Freddie'
                    AND e.last_name = 'Williams'
                    GROUP BY e.first_name, e.last_name, p.rate""").df()

cat_1809_vol = duckdb.sql("""SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                         SUM(e.hours) AS Hours,
                         p.rate AS Rate,
                         p.rate * SUM(e.hours) AS Pay
                         FROM extract e
                         JOIN people p ON p.id = e.name_id
                         WHERE e.category = '1809 Lodi'
                         AND e.first_name NOT IN ('Antonio', 'Mo', 'Sa', 'Freddie')
                         AND e.last_name NOT IN ('Joe', 'It', 'Williams')
                         GROUP BY e.first_name, e.last_name, p.rate
                         ORDER BY e.first_name""").df()

cat_1809_vol_total = duckdb.sql("""WITH cat_1809_vol AS (
                                SELECT e.first_name||' '||REPLACE(e.last_name,'none','') AS Name,
                                SUM(e.hours) AS Hours,
                                p.rate AS Rate,
                                p.rate * SUM(e.hours) AS Pay
                                FROM extract e
                                JOIN people p ON p.id = e.name_id
                                WHERE e.category = '1809 Lodi'
                                AND e.first_name NOT IN ('Antonio', 'Mo', 'Sa', 'Freddie')
                                AND e.last_name NOT IN ('Joe', 'It', 'Williams')
                                GROUP BY e.first_name, e.last_name, p.rate
                                ORDER BY e.first_name)
                                SELECT SUM(Hours) AS 'TOTAL HOURS', SUM(Pay) AS 'TOTAL EQUIVALENT CASH' FROM cat_1809_vol""").df()

people_1016 = duckdb.sql("""SELECT first_name, last_name FROM extract
                         WHERE category = '1016 N. Townsend'
                         GROUP BY first_name, last_name
                         ORDER BY first_name""").df()

st.write('# YESHUA RESTORATION MINISTRIES (YRM) DATABASE REPORT')

st.write("1. Extract of YRM's Database")
st.write(extract)

st.write("2. Extract of Lou's Records from YRM's Database")
st.write(lou)

st.write("3. Extract of Hassan Adam's Records from YRM's Database")
st.write(hassan)

st.write('4. Lou performed 247.5 hours of Specific Assistance to Hassan from May 2021 to Feb 1st, 2024.')
st.table(lou_to_hassan)

st.write('5. Hassan volunteered 213.75 hours from Jun 2021 to Feb 1st, 2024.')
st.table(hassan_hours)

st.write('6. Number of hours worked by individuals paid by YRM Dev Corp at 1800 Lodi from Jun 2017 to Feb 1st, 2024.')
st.table(cat_1800_cash)
st.table(cat_1800_cash_total)

st.write('7. Number of hours worked by individuals paid by CNY Works at 1800 Lodi from Jun 2017 to Feb 1st, 2024.')
st.table(cat_1800_cny)
st.table(cat_1800_cny_total)

st.write('8. Number of hours volunteers have worked at 1800 Lodi from Jun 2017 to Feb 1st, 2024.')
st.table(cat_1800_vol)
st.table(cat_1800_vol_total)

st.write('9. Number of hours worked by individuals paid by YRM Dev Corp at 1809 Lodi from Dec 2020 to Feb 1st. 2024.')
st.table(cat_1809_cash)
st.table(cat_1809_cash_total)

st.write('10. Freddie Williams worked 86 hours total at 1809 Lodi from Feb 2021 to Jun 2022; paid by CNY Works.')
st.table(fw_1809)

st.write('11. Number of hours volunteers have worked at 1809 Lodi from Dec 2020 to Feb 1st, 2024.')
st.table(cat_1809_vol)
st.table(cat_1809_vol_total)
st.write('*Comment: Jessica’s hours start December 2020 and end May 14, 2021.*')