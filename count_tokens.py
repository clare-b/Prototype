import psycopg2
import re
import math
from collections import defaultdict

def  main():
    conn_string = "host='localhost' dbname='doit2' user='tamr'"
    print "connecting to database\n	->%s" % (conn_string)
	# get a connection
    conn = psycopg2.connect(conn_string)
    conn.autocommit=True
    cursor = conn.cursor()
    print "connected!\n"

    cursor.execute("select part_description1,part_description2,human_match_prob from staging.data_fixed")
    records = cursor.fetchall()

    matches = defaultdict(int)
    non_matches = defaultdict(int)

    counter = 0
    n_matches = 0
    n_nonmatches = 0
    list_of_tokens=[]
    for record in records:
        tokens1=re.findall(r"[\w']+", record[0])
        tokens2=re.findall(r"[\w']+", record[1])
        tokens_in_both=set(tokens1).intersection(tokens2)

        counter_to_use = matches if record[2] >= 0.5 else non_matches

        for token in tokens_in_both:
            counter_to_use[token]+=1
            if token not in list_of_tokens:
                list_of_tokens.append(token)

        if record[2] >= 0.5:
            n_matches += 1
        else:
            n_nonmatches += 1

    
    cursor.execute("drop table if exists test.tokens_info")
    cursor.execute("create table test.tokens_info(token text, entropy numeric)")
    for token in list_of_tokens:
        #print 'matches: ',matches[token]
        #print 'non matches: ',non_matches[token]
        total=float(matches[token]+non_matches[token])
        prob_m=float(matches[token])/total
        prob_n=float(non_matches[token])/total
        #print 'prob match: ',prob_m,' prob non-match: ',prob_n
        if prob_m>0.00001 and prob_n>0.00001 and total>10:
            entropy=-prob_m*math.log(prob_m,2)-prob_n*math.log(prob_n,2)
            print 'token: ',token,' entropy: ',entropy
            cursor.execute("insert into test.tokens_info values('"+token+"',"+str(entropy)+")")

    print "Matches Size: ", sum(v for (k, v) in matches.items())
    print "NonMatches Size: ", sum(v for (k, v) in non_matches.items())
    print "Matches : ", n_matches
    print "NonMatches: ", n_nonmatches

if __name__ == "__main__":
    main()
