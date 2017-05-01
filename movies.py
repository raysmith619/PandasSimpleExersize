from __future__ import print_function
import pandas as pd
import os

# Read the CSV into a pandas data frame (df)
#   With a df you can do many things
#   most important: visualize data with Seaborn
use_sample = True           # Use(process) sample files as input
use_sample = False          # Don't  use sample for input
movie_dir = r"c:/MovieLens"
print("movie directory: {}".format(movie_dir))
sample_dir = os.path.join(movie_dir, "sample")
print("sample directory: {}".format(sample_dir))
sample_nline = 100          # Number of lines in sample

proc_dir = movie_dir        # process directory - default = movie_dir


create_sample = not use_sample        # Create sample files

if use_sample:
    create_sample = False
    proc_dir = sample_dir

print("Processing directory: {}".format(proc_dir))    
if create_sample:
    print("Creating sample files in {}".format(sample_dir))
    print("{} lines in each sample".format(sample_nline))
    if not os.path.exists(sample_dir):
        os.makedirs(sample_dir)
dfs = {}
print("Loading files from: {}".format(proc_dir))
for file_name in os.listdir(proc_dir):
    if file_name.endswith(".csv"):
        print("\nFile: {}".format(file_name))
        path = os.path.join(proc_dir, file_name)
        df = pd.read_csv(path)
        print("Headings: {}".format(list(df)))
        print("{} lines".format(len(df)))
        dfs[file_name] = df
        #print(df.tail())
        print(df.head())
        if create_sample:
            sample_file_name = os.path.join(sample_dir, file_name)
            dfsample = df           # reduced if appropriate
            if 'movieId' in df.columns:
                dfsample = dfsample[dfsample['movieId'] <= sample_nline]
            dfsample[:sample_nline].to_csv(sample_file_name, index=False)
print("End of loading files")

movies = dfs['movies.csv']
tags = dfs['tags.csv']
ratings = dfs['ratings.csv']

mjt = pd.merge(movies,tags, on='movieId')
mjt.to_csv(os.path.join(sample_dir,'mjt'), index=False)
print("Movies & Tags:\n{}".format(mjt[:10]))

def large_merge():
    """
    Reduce memory usage for mjtr = pd.merge(mjt,ratings, on='movieId')
    by doing in parts
        #http://stackoverflow.com/a/39786538/2901002
        #df1 = df1[(df1.start <= df1.start_key) & (df1.end <= df1.end_key)]
    when trying via movies,tags split then merge with ratings we get very slow vals 1..9 then hang
    or VERY slow.
    code:
    mjtr_dfs = []
    unique_vals = mjt.movieId.unique()
    nval = len(unique_vals)
    for val in unique_vals:
        mjtr_df1 = pd.merge(df[df.movieId==val], ratings, on='movieId', how='outer', suffixes=('','_key'))
        #http://stackoverflow.com/a/39786538/2901002
        #df1 = df1[(df1.start <= df1.start_key) & (df1.end <= df1.end_key)]
        if val <= 3:
            print("for val={}\n{}".format(val,mjtr_df1))
        else:
            print("val = {} of {}\r".format(val, nval))
        mjtr_dfs.append(mjtr_df1)
    Try spliting ratings, which is very much bigger on movieId
    First Attempt with typo, using df instead of mjt
    After typo correction - just as slow, stoping after about val = 7
    Try merg ratings with mjt
    Try two level split ratings then mjt
    """
    ratings_dfs = []
    ratings_unique_vals = ratings.movieId.unique()
    ratings_nval = len(ratings_unique_vals)
    for ratings_val in ratings_unique_vals:
        mjt_dfs = []
        mjt_unique_vals = mjt.movieId.unique()
        mjt_nval = len(mjt_unique_vals)
        for mjt_val in mjt_unique_vals:
            mjt_df1 = pd.merge(ratings[ratings.movieId==mjt_val], mjt[mjt.movieId==mjt_val],
                                on='movieId', how='outer', suffixes=('','_key'))
            if mjt_val <= 3:
                print("for ratings_val ={} mjt_val={}\n{}".format(ratings_val, mjt_val,mjt_df1))
            else:
                print("for ratings_val ={} mjt_val={}\r".format(ratings_val, mjt_val))
            mjt_dfs.append(mjt_df1)
        ratings_dfs.extend(mjt_dfs)
    mjtr = pd.concat(ratings_dfs, ignore_index=True)
    
    mjtr.to_csv(os.path.join(sample_dir,'mjtr'), index=False)
    print("Movies & Tags & Ratings:\n{}".format(mjtr[:10]))

do_large_merge = False
if do_large_merge:
    large_merge()
        
print("End of Test")

