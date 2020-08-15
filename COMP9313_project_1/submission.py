## import modules here

########## Question 1 ##########
# do not change the heading of the function
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    numcand = 0
    while(numcand < beta_n):
        offset += 1
        tmp = data_hashes.flatMap(lambda x: [x[0]] if (count_match(x[1],query_hashes,offset) >= alpha_m) else [])
        numcand = tmp.count()
        
    return tmp
    
    
    
    
def count_match(data,query_hashes,offset):
    counter = 0
    length = len(query_hashes)
    for i in range(length):
        if(abs(data[i]-query_hashes[i]) <= offset):
            counter += 1
    
    return counter


    
    
    
    
