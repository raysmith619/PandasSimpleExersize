from __future__ import print_function
from collections import defaultdict
import pandas as pd
import os

"""
movie short data (1Meg) processing
Read the .dat files
"""

"""
Helper functions
"""
def to_table(data_frame, file_name, nline=100, sep="::", header=False):
    """
    Write portion of data frame to sample file
    This function is to support multiple character to_csv() which appears
    not to be supported
    data_frame - dataFrame to write
    file_name - file name
    nline - number of lines to write
    sep - field separator which may be more than one character
    header - include column headers - Default: False
    """
    data_str = '\n'.join(sep.join(str(r) for r in rec) for rec in df[:nline].to_records())
    with open(file_name, 'w') as f:
        if header:
            header_str = sep.join(df.keys()) + "\n"
            f.write(header_str)
        f.write(data_str)
    
use_sample = True           # Use(process) sample files as input
use_sample = False          # Don't  use sample for input
movie_dir = r"c:/MovieLens/ml-1m"
print("movie directory: {}".format(movie_dir))
sample_dir = os.path.join(movie_dir, "sample")
print("sample directory: {}".format(sample_dir))
sample_nline = 100          # Number of lines in sample

proc_dir = movie_dir        # process directory - default = movie_dir


create_sample = not use_sample        # Create sample files

if use_sample:
    create_sample = False
    proc_dir = sample_dir

files_data = defaultdict()
movie_list = ["movies.dat", "users.dat", "ratings.dat"]
files_data["ratings.dat"] = {'headings' : 'UserID::MovieID::Rating::Timestamp'.split('::')}
files_data["users.dat"] = {'headings' : 'UserID::Gender::Age::Occupation::Zip-code'.split('::')}
files_data["movies.dat"] = {'headings' : 'MovieID::Title::Genres'.split('::')}

print("Processing directory: {}".format(proc_dir))    
if create_sample:
    print("Creating sample files in {}".format(sample_dir))
    print("{} lines in each sample".format(sample_nline))
    if not os.path.exists(sample_dir):
        os.makedirs(sample_dir)
dfs = {}
print("Loading files from: {}".format(proc_dir))
for file_name in movie_list:
    print("\nFile:{}".format(file_name))
    path = os.path.join(proc_dir, file_name)
    file_data = files_data[file_name]
    headings = file_data['headings']
    df = pd.read_table(path, sep='::', header=None, names=headings)
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
        to_table(dfsample, sample_file_name, nline=sample_nline)    
print("End of loading files")

movies = dfs['movies.dat']
users = dfs['users.dat']
ratings = dfs['ratings.dat']

ratings_users = pd.merge(ratings, users)
print("\nRatings & Users:\n{}".format(ratings_users[:10]))
sample_path = os.path.join(sample_dir,'ratings_users.dat')
to_table(ratings_users, sample_path, nline=sample_nline, header=True)

ratings_users_movies = pd.merge(ratings_users, movies)
print("\nRatings + users + movies:\n{}".format(ratings_users_movies[:10]))
sample_path = os.path.join(sample_dir,'ratings_users_movies.dat')
to_table(ratings_users_movies, sample_path, nline=sample_nline, header=True)

"""
Works on Pandas 18
mean_ratings = ratings_users_movies.pivot_table('Rating',
                            columns=['Title','Gender'], aggfunc='mean')
"""
"""
The following follows the Python for Data Analysis example, but with the keys
capitalized as seen from the data set.
Also I had to take the advice from Chris Snow's online remark:
The solution for me was to change 'rows=>index' and 'cols=>columns'
It appears that the names used in the book are deprecated and removed.
"""
mean_ratings = ratings_users_movies.pivot_table('Rating', index='Title',
                            columns='Gender', aggfunc='mean')

print(mean_ratings[:5])        

ratings_by_title = ratings_users_movies.groupby('Title').size()
print("\nratings by title\n{}".format(ratings_by_title[:10]))
print("\tNOTE: groupby('title') gives key error\n")


active_titles = ratings_by_title.index[ratings_by_title >= 250]
print("active_titles:\n{}".format(active_titles))

mean_ratings_active = mean_ratings.ix[active_titles]
print("\nmean_ratings_active:\n{}".format(mean_ratings_active[:10]))

top_female_ratings = mean_ratings_active.sort_values(by='F', ascending=False)
print("\ntop_female_ratings:\n{}".format(top_female_ratings[:10]))

mean_ratings_active['diff'] = mean_ratings_active['M'] - mean_ratings_active['F']
sorted_by_diff = mean_ratings_active.sort_values(by='diff')
print("\nactive sorted_by_diff:\n{}".format(sorted_by_diff[::-1][:15]))

rating_std_by_title = ratings_users_movies.groupby('Title')['Rating'].std()
rating_std_by_title_active =rating_std_by_title.ix[active_titles]
rating_std_by_title_active_order = rating_std_by_title_active.sort_values(ascending=False)[:10]
print("\nRating of Active Std Deviation".format(rating_std_by_title_active_order))

print("End of Test")
