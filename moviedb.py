import pymysql
from prettytable import PrettyTable 
import pandas as pd
import json
#   def InsertnormalTup, for normal relations
def InsertnormalTup(cur, data, string1, tableID, tablename, relationName, idthing):
    list_Nore = []
    sql = "INSERT INTO "
    sql = sql + relationName + "(" + tableID + "," + tablename + ") " + "VALUES (%s, %s)"
    
    for row in (data[string1]):
        noggin = json.loads(row)
        for dict in noggin:
            if dict[idthing] not in list_Nore: 
                list_Nore.append(dict[idthing])
                cur.execute(sql,( dict[idthing], dict["name"].replace("\"", "\\\"") )  )  
#    ===================================================================================

#   def InsertnormalTup, for weak relations
def Insert_ManyTup(cur, data, string1, tableID1, movieID, relationName, idthing):
    sql = "INSERT INTO "
    sql = sql + relationName + "(" + tableID1 + "," + movieID + ") " + "VALUES (%s, %s)"
    
    loopNum = 0
    for row in (data["id"]):
        noggin = json.loads(data[string1][loopNum])
        loopNum = loopNum + 1
        
        for dict in noggin: 
            cur.execute(sql,( dict[idthing], row) )  
#    ==============================================
    
# def insertData
def insertData(conn, cur):
    data = pd.read_csv ('tmdb_5000_movies.csv')   
    data.fillna(pymysql.NULL, inplace=True)

#   proof somethings X_X, movie relation
    loopNum = 0
    for row in data["id"]:
        # print(loopNum)
        sql = "INSERT INTO Movie(movie_ID, title, budget, homepage, original_language, original_title, overview, popularity, release_date, revenue,runtime, m_status, tagline, vote_avg, vote_count ) VALUES (%s,%s,%s,  %s,%s,%s,  %s,%s,%s,  %s,%s,%s, %s,%s,%s)"
        
        if data["revenue"][loopNum] == pymysql.NULL:
            tempRevenue = 0
        else:
            tempRevenue = data["revenue"][loopNum]
            
        if data["runtime"][loopNum] == pymysql.NULL:
            tempRevenue = 0
        else:
            tempRunTime = data["runtime"][loopNum]
      
        
        if data["budget"][loopNum] == pymysql.NULL:
            tempBudget = 0
        else:
            tempBudget = data["budget"][loopNum]
        
        if data["vote_count"][loopNum] == pymysql.NULL:
            tempCount = 0
        else:
            tempCount = data["vote_count"][loopNum]
            
        if data["vote_average"][loopNum] == pymysql.NULL:
            tempAvgCount = 0
        else:
            tempAvgCount = data["vote_average"][loopNum]
        
        cur.execute(sql, (data["id"][loopNum], data["title"][loopNum],tempBudget, 
                          data["homepage"][loopNum], data["original_language"][loopNum], data["original_title"][loopNum],
                          data["overview"][loopNum], data["popularity"][loopNum], data["release_date"][loopNum],
                          tempRevenue,tempRunTime, data["status"][loopNum], 
                          data["tagline"][loopNum], tempAvgCount, tempCount) )
        conn.commit()
        loopNum = loopNum + 1
    
    # print("final " + str(loopNum))
    
# #   insert normal relations 
    InsertnormalTup(cur, data,"genres", "Genre_ID", "Genre_name","Genre", "id")
    conn.commit()       
    InsertnormalTup(cur, data,"keywords", "Keywords_ID", "Keywords_name","Keywords", "id")
    conn.commit()       
    InsertnormalTup(cur, data,"production_companies", "ProductionCompany_ID", "ProductionCompany_name","ProductionCompany",  "id")
    conn.commit()       
    InsertnormalTup(cur, data,"production_countries", "iso_3166_1_ID", "ProductionCountries_name","ProductionCountries", "iso_3166_1")
    conn.commit()       
    InsertnormalTup(cur, data,"spoken_languages", "iso_639_1_ID", "SpokenLanguages_name","SpokenLanguages", "iso_639_1")
    conn.commit()      
     
# #   insert into weak relations
    Insert_ManyTup(cur, data,"spoken_languages", "iso_639_1_ID", "Movie_ID","SpokenLanguages_movies", "iso_639_1")
    conn.commit()
    Insert_ManyTup(cur, data,"production_countries", "iso_3166_1_ID", "Movie_ID","ProductionCountries_movies", "iso_3166_1")
    conn.commit()   
    Insert_ManyTup(cur, data,"production_companies", "ProductionCompany_ID", "Movie_ID","ProductionCompany_movies", "id")
    conn.commit()   
    Insert_ManyTup(cur, data,"keywords", "Keywords_ID", "Movie_ID","Keywords_movies", "id")
    conn.commit()   
    Insert_ManyTup(cur, data,"genres", "Genre_ID", "Movie_ID","Genre_movie", "id")
    conn.commit()   
# #   =============

#   create tables
def createTables(conn, cur):
    sql = """CREATE TABLE Movie(
            movie_ID int, 
            title varchar(600) not null, 
            budget int, 
            homepage varchar(600), 
            original_language varchar(20), 
            original_title varchar(600), 
            overview varchar(1500), 
            popularity Float, 
            release_date varchar(600), 
            revenue bigint, 
            runtime int, 
            m_status varchar(600), 
            tagline varchar(600), 
            vote_avg FLOAT, 
            vote_count int,
            PRIMARY KEY (movie_ID)
            );"""
    cur.execute(sql)
    conn.commit()
    
    sql ="""
            CREATE TABLE Genre(
            Genre_ID int,
            Genre_name varchar(600),
            PRIMARY KEY (Genre_ID)
            );"""
    cur.execute(sql)
    conn.commit()
    
    sql ="""
           CREATE TABLE Keywords(
            Keywords_ID int, 
            Keywords_name varchar(600),
            PRIMARY KEY (Keywords_ID)
            ); 
            """
    cur.execute(sql)
    conn.commit()
    
    sql ="""
            CREATE TABLE ProductionCompany(
            ProductionCompany_ID int,
            ProductionCompany_name varchar(600),
            PRIMARY KEY (ProductionCompany_ID)
            ); 
            """
    cur.execute(sql)
    conn.commit()

    sql ="""
           CREATE TABLE ProductionCountries(
            iso_3166_1_ID char(2),
            ProductionCountries_name varchar(600),
            PRIMARY KEY (iso_3166_1_ID)
            ); 
            """
    cur.execute(sql)
    conn.commit()

    sql ="""
            CREATE TABLE SpokenLanguages(
            iso_639_1_ID char(2),
            SpokenLanguages_name varchar(600),
            PRIMARY KEY (iso_639_1_ID)
            ); 
            """
    cur.execute(sql)
    conn.commit()

    sql ="""
            CREATE TABLE Genre_movie(
            Genre_ID int,
            Movie_ID int,
            foreign key (Genre_ID) REFERENCES Genre(Genre_ID), 
            foreign key (Movie_ID) REFERENCES Movie(Movie_ID),
            primary key(Genre_ID, Movie_ID)
            ); 
            """
    cur.execute(sql)
    conn.commit()

    sql ="""
            CREATE TABLE Keywords_movies(
            Keywords_ID int, 
            Movie_ID int,
            foreign key (Keywords_ID) REFERENCES Keywords(Keywords_ID), 
            foreign key (Movie_ID) REFERENCES Movie(Movie_ID),
            primary key(Keywords_ID, Movie_ID)
            ); 
            """
    cur.execute(sql)
    conn.commit()

    sql ="""
            CREATE TABLE ProductionCompany_movies(
            ProductionCompany_ID int,
            Movie_ID int,
            foreign key (ProductionCompany_ID) REFERENCES ProductionCompany(ProductionCompany_ID), 
            foreign key (Movie_ID) REFERENCES Movie(Movie_ID),
            primary key(ProductionCompany_ID, Movie_ID)
            ); 
            """
    cur.execute(sql)
    conn.commit()

    sql ="""
            CREATE TABLE ProductionCountries_movies(
            iso_3166_1_ID char(2),
            Movie_ID int,
            foreign key (iso_3166_1_ID) REFERENCES ProductionCountries(iso_3166_1_ID),
            foreign key (Movie_ID) REFERENCES Movie(Movie_ID),
            primary key(iso_3166_1_ID, Movie_ID)
            ); 
            """
    cur.execute(sql)
    conn.commit()

    sql ="""
            CREATE TABLE SpokenLanguages_movies(
            iso_639_1_ID char(2),
            Movie_ID int, 
            foreign key (iso_639_1_ID) REFERENCES SpokenLanguages(iso_639_1_ID),
            foreign key (Movie_ID) REFERENCES Movie(Movie_ID),
            primary key(iso_639_1_ID, Movie_ID)
            ); """
    cur.execute(sql)
    conn.commit()
#   def create tables()
#   ===================

#   part 1 query1 
def query1(conn, cur):
    sql = """select avg(budget) from movie; """
    cur.execute(sql)
    conn.commit()
    
    queryRate = cur.fetchall()
    for row in queryRate:
        print(row[0])
   
#   part 2 query2
def query2(conn, cur): 
    sql = """select title, ProductionCompany_name  
            from ( productioncompany natural join (productioncountries natural join (movie natural join productioncountries_movies) natural join  productioncompany_movies) )
            where ProductionCountries_name = 'United States of America' """
    cur.execute(sql)
    conn.commit()

    queryRate = cur.fetchall()
    for row in queryRate:
        print(row[0] + "," + row[1])

#   part 3 query3
def query3(conn, cur): 
    sql = """select title, revenue 
            from movie 
            order by revenue desc 
            limit 5"""
    cur.execute(sql)
    conn.commit()
    
    queryRate = cur.fetchall()
    for row in queryRate:
        print(row[0] + "," + str (row[1]))

#   part 4 query4
def query4(conn, cur): 
    sql = """select title, Genre_name
            from genre natural join (movie natural join genre_movie)
            where title in 
            (select title
            from genre natural join (movie natural join genre_movie)
            where Genre_name = 'Science Fiction' )
            and title in
            (select title
			from genre natural join (movie natural join genre_movie)
            where Genre_name = 'Mystery')"""
    cur.execute(sql)
    conn.commit()
    
    queryRate = cur.fetchall()
    for row in queryRate:
        print(row[0] + "," + row[1])
        
#   part 5 query5
def query5(conn, cur): 
    sql = """select title, popularity 
            from movie 
            where popularity > (select avg(popularity) from movie)"""
    cur.execute(sql)
    conn.commit()
    
    queryRate = cur.fetchall()
    for row in queryRate:
        print(row[0] + "," + str(row[1]))
        
#   def querying()
#   ==============

def main():
    
#   connection
    conn = pymysql.connect(host='localhost', port=, user='root', passwd='')
    cur = conn.cursor()

#   choose database
    sql = "" 
    cur.execute(sql)
    
# #   create tables
    createTables(conn, cur)

# #   insert into table
    insertData(conn, cur)
    
#   querying
    query1(conn, cur)
    query2(conn, cur)
    query3(conn, cur)
    query4(conn, cur)
    query5(conn, cur)
    
    conn.commit()         
    cur.close()
    conn.close()
#   ============
#   main()
#   ======

if __name__ == '__main__':
    main()