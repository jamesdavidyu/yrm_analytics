import duckdb
from os import getenv
import pandas as pd
import psycopg
import streamlit as st
import matplotlib.pyplot as plt

# TODO: fetch data from api

def fetch_extract():
    user = getenv("PGUSER")
    password = getenv("PGPASSWORD")
    host = getenv("PGHOST")
    port = getenv("PORT")
    database = getenv("PGDATABASE")
    cloud = psycopg.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
        sslmode="require"
    )

    with cloud.cursor() as cursor:
        cursor.execute(
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
                ORDER BY r.date;
            """
        )
        df = pd.DataFrame(cursor.fetchall(), columns=['id', 'date', 'category', 'first_name', 'last_name', 'role', 'activity', 'hours', 'money', 'comments', 'category_id', 'name_id', 'role_id', 'activity_id'])

    cloud.close()

    return df

def fetch_people():
    user = getenv("PGUSER")
    password = getenv("PGPASSWORD")
    host = getenv("PGHOST")
    port = getenv("PORT")
    database = getenv("PGDATABASE")
    cloud = psycopg.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
        sslmode="require"
    )

    with cloud.cursor() as cursor:
        cursor.execute('SELECT * FROM people;')
        df = pd.DataFrame(cursor.fetchall(), columns=['id', 'first_name', 'last_name', 'dob', 'rate'])
    
    cloud.close()

    return df

extract = fetch_extract()

people = fetch_people()

lou = duckdb.sql("""SELECT date, first_name, last_name, category, activity as notes, hours FROM extract
                 WHERE first_name = 'Lou' AND category = 'Specific Assistance' AND activity LIKE 'Hassan%'""").df()

hassan = duckdb.sql("""SELECT date, first_name, last_name, category, activity as notes, hours FROM extract
                    WHERE first_name = 'Hassan'
                    AND YEAR(date) >= 2022""").df()

lou_to_hassan = duckdb.sql("""SELECT first_name||' '||last_name AS Name, SUM(hours) AS 'Total Hours' FROM extract
                           WHERE category = 'Specific Assistance' AND activity LIKE 'Hassan%' AND YEAR(date) >= 2021
                           GROUP BY first_name, last_name""").df()

hassan_hours = duckdb.sql("""SELECT first_name||' '||last_name AS Name, SUM(hours) AS 'Total Hours' FROM extract
                           WHERE first_name = 'Hassan' AND YEAR(date) >= 2022
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

hours_1016 = duckdb.sql("""SELECT category AS Category, SUM(hours) as Hours FROM extract
                        WHERE category = '1016 N. Townsend'
                        GROUP BY category""").df()

hours_vol_1016 = duckdb.sql("""SELECT first_name || ' ' || (CASE WHEN last_name = 'none' THEN '' ELSE last_name END) AS Name, SUM(hours) AS Hours
                            FROM extract
                            WHERE category = '1016 N. Townsend'
                            GROUP BY Name
                            ORDER BY Name""").df()

hours_yearly_1016 = duckdb.sql("""SELECT YEAR(date) AS Year, SUM(hours) AS Hours
                               FROM extract
                               GROUP BY Year
                               ORDER BY Year""").df()

fig, ax = plt.subplots()
bars = ax.bar(hours_yearly_1016['Year'], hours_yearly_1016['Hours'])
ax.set_title('3. Total hours worked on 1016 N. Townsend by year from Apr 2016 to Dec 2024.')
ax.set_xlabel('Year')
ax.set_ylabel('Hours')
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, height, f'{height}', ha='center', va='bottom')

check_1016 = duckdb.sql("SELECT * FROM extract WHERE category = '1016 N. Townsend'").df()

st.write('# YESHUA RESTORATION MINISTRIES (YRM) DATABASE REPORT')

# st.write(check_1016)

st.write('1. Total hours worked on 1016 N. Townsend from Apr 2016 to Dec 2024.')
st.table(hours_1016)

st.write('2. Total hours worked on 1016 N. Townsend per volunteer from Apr 2016 to Dec 2024.')
st.table(hours_vol_1016)

st.pyplot(fig)
st.table(hours_yearly_1016)

# st.write("1. Extract of YRM's Database")
# st.write(extract)

# st.write("1. Extract of Lou's Specific Assistance to Hassan Records from YRM's Database")
# st.table(lou)

# st.write("2. Extract of Hassan Adam's Records from YRM's Database")
# st.table(hassan)

# st.write('3. Hours of Specific Assistance to Hassan performed by Lou from Nov 2022 to Dec 2024.')
# st.table(lou_to_hassan)

# st.write('4. Hours volunteered by Hassan from Nov 2022 to Dec 2024.')
# st.table(hassan_hours)

# st.write('6. Number of hours worked by individuals paid by YRM Dev Corp at 1800 Lodi from Jun 2017 to Feb 1st, 2024.')
# st.table(cat_1800_cash)
# st.table(cat_1800_cash_total)

# st.write('7. Number of hours worked by individuals paid by CNY Works at 1800 Lodi from Jun 2017 to Feb 1st, 2024.')
# st.table(cat_1800_cny)
# st.table(cat_1800_cny_total)

# st.write('8. Number of hours volunteers have worked at 1800 Lodi from Jun 2017 to Feb 1st, 2024.')
# st.table(cat_1800_vol)
# st.table(cat_1800_vol_total)

# st.write('9. Number of hours worked by individuals paid by YRM Dev Corp at 1809 Lodi from Dec 2020 to Feb 1st. 2024.')
# st.table(cat_1809_cash)
# st.table(cat_1809_cash_total)

# st.write('10. Freddie Williams worked 86 hours total at 1809 Lodi from Feb 2021 to Jun 2022; paid by CNY Works.')
# st.table(fw_1809)

# st.write('11. Number of hours volunteers have worked at 1809 Lodi from Dec 2020 to Feb 1st, 2024.')
# st.table(cat_1809_vol)
# st.table(cat_1809_vol_total)
# st.write('*Comment: Jessica’s hours start December 2020 and end May 14, 2021.*')