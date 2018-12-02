import pandas as pd

cities = pd.read_csv("../input/cities.csv")


def is_prime(n):
    if n>2:
        i=2
        while i ** 2 <=n:
        #如果不能被i整除，那么就加1，
            if n % i:
                i+=1
            #如果在自增的过程中n被i整出了，那么一定不是中心城市
            else :
                return False
    #如果n为1或0，则也不是中心城市
    elif n !=2:
        return False
    return True

cities['is_prime']=cities.CityId.apply(is_prime)