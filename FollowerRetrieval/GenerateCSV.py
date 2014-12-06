import psycopg2
import csv, re
import xlsxwriter
import datetime

def generate_CSV(target_user, twitter_users_sets, rows):
    f = open('{0}.csv'.format(target_user), 'wt')
    try:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(('Twitter profile', 'name', 'Followers count', 'Following', 'Tweets Count', 'On Twitter Since', 'Follows MrDrewScott', 'Follows MrSilverScott'))
        # get followers that don't have info stored
        for row in rows:
            com = ()
            for key in twitter_users_sets:
                    com += ('YES',) if row[0] in twitter_users_sets[key] else ('',)
            writer.writerow(('https://twitter.com/' + row[1],) + row[2:7] + com)
    finally:
        f.close()
        
def generate_XLSX(target_user, twitter_users_sets, rows):
    
    # Create a workbook and add a worksheet.
    workbook = xlsxwriter.Workbook('{0}.xlsx'.format(target_user))
    worksheet = workbook.add_worksheet()
    
    # Start from the first cell. Rows and columns are zero indexed.
    row = 0
    col = 0
    header = ['Name', 'Followers count', 'Following', 'Tweets Count', 'On Twitter Since', 'Follows MrDrewScott', 'Follows MrSilverScott']
    for item in header:
        worksheet.write(row, col, item)
        col += 1
    
    row += 1
    for item in rows:
        try:
            com = ()
            for key in twitter_users_sets:
                com += ('YES',) if item[0] in twitter_users_sets[key] else ('',)
            #name hyperlink
            link_format = workbook.add_format({'color': 'blue', 'underline': 1})
            worksheet.write_url(row, 0, 'https://twitter.com/' + item[1],link_format,item[2].decode('utf-8'))
            #follower count
            worksheet.write(row,1, item[3])
            #friends count
            worksheet.write(row,2, item[4])
            #tweet count
            worksheet.write(row,3, item[5])
            #created at count
            t = item[6].replace(tzinfo=None)
            date_format = workbook.add_format({'num_format': 'd mmmm yyyy'})
            worksheet.write_datetime(row,4, t,date_format)
            #follows users
            for x in range(0,len(com)):
                worksheet.write(row,5+x, com[x])
            
            row+= 1
        except Exception as e:
            print item
            print "Unexpected error:", e
    
    workbook.close()

if __name__ == '__main__':
    # open data base communications
    conn = psycopg2.connect("dbname=socialpeeks user=postgres")
    # Open a cursor to perform database operations
    cur = conn.cursor()
    
    twitter_users = ['MrDrewScott', 'MrSilverScott']
    target_users = ['bromco','Make_It_Right','ChrisLambton13','VernYipDesigns','em_henderson','SarahR_Design','sabrinasoto']
    
    twitter_users_sets = {}
    
    for twitter_user in twitter_users:
        # get followers that don't have info stored
        cur.execute("select follower_id from followers where twitter_user=%s and dead_follower=FALSE", (twitter_user,)) 
        rows = cur.fetchall()
        users = []
        for row in rows:
                users += row
        twitter_users_sets[twitter_user] = users
    
    for target_user in target_users:
        cur.execute("select follower_id,screen_name,name,followers_count,friends_count,statuses_count,created_at from followers where twitter_user=%s and dead_follower=FALSE order by followers_count desc", (target_user,)) 
        rows = cur.fetchall()
        # generate_CSV(target_user,twitter_users_sets,rows)
        generate_XLSX(target_user, twitter_users_sets, rows[:6500])  
        print target_user   
    
    conn.close()
    print "Done!!"
    
    
    # Name
# of Followers
# Following
# of Tweets
# How long on Twitter 
