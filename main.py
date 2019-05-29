"""
    Processing path
        [List of Paths to warts files] -> 
        [List of Parsed Traceroutes] ->
        [Aggregate traceroutes with same source,destination] ->
        [Filter traceroutes with less than 2 repetitions] ->
        [Calculate fluctuation] ->
        Write data to a file


    Misc
        Stop reason
            1 -> Done
            2 -> icmp unreach host prohib
            3 -> ?
            4 -> Loop
            5 -> Gap Limit Exceeded
"""




import sys,os

#spark
from pyspark.sql import SparkSession

#list processing (fluctuation calculation)
import numpy as np

#decompression and parsing
import warts
from warts.traceroute import Traceroute


#Globals
#TODO Fix this hardcode!
path = "/home/fatima/Documents/internet_measurements/ark_analysis/team-1/analysis"

cycles = map(lambda x: path+'/'+x ,os.listdir(path))
path_to_files = []

for cycle in cycles:
    path_to_files = path_to_files + map( lambda x: cycle+'/'+x, os.listdir(cycle) )


"""
    [List] -> Int
        returns a measure of fluctuation of values in the list
"""


def fluctutaion(temp_list):
    if len(temp_list) < 2:
        return 0
    else:
        return np.mean(map(lambda x: abs(x - temp_list[0]),temp_list)[1:])




"""
    String -> [List]
        Takes in a path to traceroute .warts file and returns a list of valid traceroutes [((src_address,dst_address), path_length)]
        Where (src_address, dst_address) is key and path_length is value

    This function uses minimal amount of memory, streams over the compressed binary.
"""



def parse_warts_file(path_to_file):
    trace_routes = []
    try:    
    
        with open(path_to_file, 'rb') as f:
            while True:
                record = warts.parse_record(f)
                if record:
                    if type(record) is Traceroute and record.stop_reason == 1:
                        trace_routes.append ( ( (record.src_address,record.dst_address), record.nb_hops) )
                else:
                    break
        return trace_routes
    
    except:
        print "Failed to parse/read file {}".format(path_to_file.split("/")[-1])
        return []

"""
    Main function

"""


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ark_analysis").getOrCreate() # build spark session
    
    data_points = spark.sparkContext.parallelize(path_to_files).flatMap(parse_warts_file).map(lambda (x,y):(x,[y])) \
        .reduceByKey(lambda a,b: a+b).filter(lambda x: len(x[1]) >= 2 ).map(lambda x: (x[0], fluctutaion(x[1])))
    
    data_points.coalesce(1, True).saveAsTextFile("test")

    spark.stop() # terminate spark session
