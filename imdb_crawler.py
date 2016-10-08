"""this script crawls the imdb site to fetch the data about movies above a mentioned rating. we need to supply the url of the first movie, the rating above which we want the data, and the number of movies we wish to crawl"""
import requests
from bs4 import BeautifulSoup
from queue import Queue
import sys
import csv

visited_movie_id=[]
to_be_visited_queue=Queue()
counter=0


print ("This script requires 3 command line arguments except the name of the script itself. ")
print ("sys.argv[1] should be the url of the movie with rating greater than or equal to the one we want. eg if we want to crawl the movies with rating greater than 7 then the first argument should be url of a movie with rating equal to or greater than 7")
print ("sys.argv[2] should be the number of movies we want to crawl and put in csv file")
print ("sys.argv[3] should be the minumum rating of the movie below which we dont need movie data")
  
  
"""this function does three things:
      1. It prints the info like name, year director etc of the movie id it recieves
      2. it creates a list of the movie ids that have a link on the current page. I call these movies friend of the current page
      3. so after the 1st 2 tasks this movie node has been visited and hence this function adds this movie id to the visited_movie_id list"""
def movie_visit(movie_id):
  url="http://www.imdb.com/title/tt"+str(movie_id)+"/"
  r=requests.get(url)
  soup=BeautifulSoup(r.text)
  frnd_list=[] # list of movie ids that have a link on the current page
  try:
    rating=soup.findAll('div',{'class':'ratingValue'})[0].span.string
    tag=soup.findAll('div',{'id':'pagecontent'})[0]['itemtype'].split('/')[-1:][0]
    if tag != 'Movie':
      return frnd_list # right now frnd list is empty
    if float(rating)>=int(sys.argv[3]):
      global counter
      counter=counter+1
      year=soup.findAll('div',{'class':'title_wrapper'})[0].h1.span.a.string
      title=soup.findAll('div',{'class':'title_wrapper'})[0].h1.stripped_strings.__next__()
      director=soup.findAll('span',{'itemprop':'director'})[0].span.string
      data_entry=[title,year,rating,director]
      writer.writerow(data_entry)#putting entry in csv file
      print (title," ",year," ",rating," ",director)
  except (IndexError, AttributeError, KeyError):
    print (" - Info aint there about this movie or this aint a movie")
  else: #here we make a list of movie ids that have a link on the current page
    frnd_container=soup.findAll('div',{'class':'rec_page'})
    for i in frnd_container:
      l=i.findAll('a')
      for i in l:
        frnd_id=i['href'].split('/')[2][2:]
        numeric_frnd_id=int(frnd_id)
        frnd_list.append(numeric_frnd_id)
  visited_movie_id.append(movie_id) #movie id is the number present in the url of a movie page
  return frnd_list



""" this bfs function performs a simple breadth first search on the graph of movie web pages. it iterates over the frnd_movie id list returned by movie visit function call on a movie id, if the friend movie id has not been visited and has not been enqued in the to_be_visited_queue already then that id is put in the queue. After the iteration movie_visit is called on a movie_id after dequeuing it from the queuw """
#command line arguments are always recieved as a string. so explicit conversion to int is required (as in 3rd line of bfs function below )
def bfs(movie_id):
  movieId=movie_id
  while(True): 
    if(counter>=int(sys.argv[2])):
      break
    frnd_list=movie_visit(movieId)
    for i in frnd_list:
      if i not in visited_movie_id and i not in to_be_visited_queue.queue:
        #nice way to check if something is there in an array or a queue.
        #if rating_checker(movieId):
        to_be_visited_queue.put(i)
    if not to_be_visited_queue.empty():
      movieId = to_be_visited_queue.get()
    else:
      print ("queue is empty")
      return
    

start_movie_id= sys.argv[1].split('/')[4][2:]
f=open('imdb_crawled_data'+sys.argv[2]+'.csv','w') #opening a file for entry s
writer=csv.writer(f)
bfs(int(start_movie_id))
f.close()   



