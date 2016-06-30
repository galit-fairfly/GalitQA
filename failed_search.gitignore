# Python

import MySQLdb
import sys

# ===================================================================
# connect to mysql
# ===================================================================

try:
    db = MySQLdb.connect(host="b2b-staging.cirsjsrxccdq.eu-west-1.rds.amazonaws.com",
                         user="rootl3r",
                         passwd="World5astage",
                         db="fair_fly")
except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0], e.args[1])
    sys.exit()

# ===================================================================
# query select from table
# ===================================================================

cursor = db.cursor()

cursor.execute("""
select
case `scan_state`
        when 0 then "QUEUED_FOR_SCAN"
        when 1 then "BEING_SCANNED"
        when 2 then "SCAN_SUCCESS"
        when 3 then "SCAN_FAILED"
        when 4 then "SCAN_TIMEOUT"
        when 5 then "SCAN_CALLBACK"
    END as real_scan_state, count(1)
from `queue_tasks_scanlog`
where `created_at` >=  NOW() - INTERVAL 1 DAY
GROUP BY `scan_state`;
""")

# _rows = ( (1,2) , (3,4) )
# dict = { 'name': 'Galit',
#          'hair_color': 'blond'}
#
# fail_rate = { 'title_1': 24, 'title_2': 345,....}


fail_dict = {}

for col_title, col_count in cursor._rows:
    fail_dict[col_title] = col_count


#print fail_dict['SCAN_SUCCESS']+fail_dict['SCAN_FAILED']+fail_dict['BEING_SCANNED']+fail_dict['QUEUED_FOR_SCAN']
#print fail_dict['SCAN_FAILED'] / float(sum(fail_dict.values())) * 100

percentage = fail_dict['SCAN_FAILED'] / float(sum(fail_dict.values())) * 100

if percentage > 10:
    print "%.2f percent of the searches failed since yesterday. See https://app.logz.io/kibana/index.html#/discover/Staging-Failed-Searches (set logz.io time to: Last 24 hours)" % percentage
else:
    print 'Great!'

cursor.close()
db.close()



# or:
# num_fields = len(cursor.description)
# field_names = [i[0] for i in cursor.description]
#https://medium.com/@julianmartinez/how-to-write-a-slack-bot-with-python-code-examples-4ed354407b98#.d083jsglp
