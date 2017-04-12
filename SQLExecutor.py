import pip

#pip.main(["install", "PyGithub"] )
#pip.main(["install", "psycopg2"] )
#sudo apt-get install libpq-dev python-dev
#pip.main(["install", "pymysql"] )

from github import Github
import psycopg2
import os
from datetime import datetime, tzinfo
from time import sleep

def get_connection():
    return psycopg2.connect( host="10.101.0.178", port=5432, user="david_asmuth", password="918214012", database="postgres" )

def main():
    last_check = datetime.min
    repo = Github( "OITDatabaseGithub", "longjohnsilver1" ).get_user().get_repo( "LabQueries" )
    
    while( True ):
        try:
            commits = repo.get_commits( since=last_check )
            if has_elements( commits ):
                check_repo( repo )
                print( "Repo checked" )
            else:
                print( "No commits" )
            last_check = datetime.utcnow()
        except:
            pass
        sleep( 10 )

def has_elements( thing ):
    for t in thing: 
        return True
    return False

def check_repo( repo ):
    for folder in repo.get_dir_contents( "/" ):
        if folder.type != 'dir':
            continue
        subs = [x.name for x in repo.get_dir_contents( folder.name + "/" )]
        if len( subs ) == 0:
            continue #they messed up, ignore then
        elif len( subs ) == 1 and subs[0] == "Scripts":
            for file in repo.get_dir_contents( folder.name + "/Scripts/" ):
                query = repo.get_file_contents( file.path ).decoded_content
                run_query_insert_result( query, repo, folder.name, file.name )
        elif len( subs ) == 2 and "Scripts" in subs and "Results" in subs:
            result_files = [no_ext( x.name ) for x in repo.get_dir_contents( folder.name + "/Results/" )]
            query_files = [str( x.name ) for x in repo.get_dir_contents( str( folder.name ) + "/Scripts/" ) if no_ext( x.name ) not in result_files]
            for file_name in query_files:
                try:
                    query = repo.get_file_contents( str( folder.name ) + "/Scripts/" + file_name ).decoded_content
                    run_query_insert_result( query, repo, str( folder.name ), file_name )
                except Exception as e:
                    print( "Tried to run query for file " + file_name + " and failed." )
                    print( e )
        else:
            continue #they messed up, ignore them

def run_query_insert_result( query, repo, folder, filename ):
    try:
        con = get_connection()
        cur = con.cursor()
        cur.execute( query )
        result = ( ",".join( [x[0] for x in cur.description] ) ) + '\n'
        result += "\n".join( [",".join( ['"' + str(col) + '"' for col in row] ) for row in cur] )
        filename = no_ext( filename ) + ".csv"    
        repo.create_file( "/" + folder + "/Results/" + filename, "Result for " + folder + ' ' + filename, result )
    except Exception as e:
        repo.create_file( "/" + folder + "/Results/" + filename, "Error for " + folder + ' ' + filename, str( e ) )

    print( "Committed " + filename )

def no_ext( filename ):
    return os.path.splitext( os.path.basename( filename) )[0]
    
if __name__ == "__main__":
    main()
