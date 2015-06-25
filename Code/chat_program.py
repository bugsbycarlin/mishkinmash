#
# Copyright 2015, Matthew Carlin
#
# Released under MIT license. Do what you want with this code; I would appreciate if you credit me!
#
# This program uses a chat database created by the accompanying chat_processor.py to search old chat
# exchanges, and turns them into a sort of chatbot; you say stuff to the chatbot, and it searches old
# chat archives for a response.
#

import datetime
import math
import random
import re
import sqlite3
import sys

word_regex = re.compile("[\w']+|[?]")

# Logs are logged by session.
session_id = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".")[0]
print "Your session session_id is %s. Your log files are in ../Logs." % session_id

# There is a plain text chat log and an html debug log.
chat_logfile = open("../Logs/%s.txt" % session_id, "w")
debug_logfile = open("../Logs/%s_debug.html" % session_id, "w")
debug_logfile.write("<html><body>\n")

database_path = "../Database/search_database"
connection = sqlite3.connect(database_path)
c = connection.cursor()

# Username is an option that can be specified at the command line.
# eg python chat_program.py Bill will start the program and give
# you the username Bill.
username = "User"
if len(sys.argv) > 1:
  username = sys.argv[1]


#
# Take the user input string and return a suitable chat response.
#
def get_response(user_input):
  # Chunk the user input string into a series of lower case words.
  words = re.findall(word_regex, user_input)
  words = [w.lower() for w in words]

  # Make ngrams out of those words.
  ngrams = {
    1: {},
    2: {}
  }

  for j in range(0, len(words)):
    ngram(words, j, 1, ngrams[1])
    if j < len(words) - 1:
      ngram(words, j, 2, ngrams[2])

  # Look up search results in the database for each of those ngrams.
  search_results = {}

  for i in ngrams.keys():
    grams = ngrams[i]
    for term in grams:
      if term not in search_results:
        result = getSearchResults(term)
        if result:
          search_results[term] = [[int(pairstring.split(",")[0]), float(pairstring.split(",")[1])] for pairstring in result[2].split("_")]
     

  # Turn the search results into a set of responses.
  responses = {} 
  for term in search_results.keys():
    for pair in search_results[term]:
      if pair[0] not in responses:
        responses[pair[0]] = 0
      responses[pair[0]] += pair[1]


  # Make the responses into a list and keep only the top 40.
  response_list = [[k,v] for k,v in responses.items()]
  response_list.sort(key = lambda x: x[1], reverse = True)
  if len(response_list) > 40:
    response_list = response_list[0:40]


  # Write a bunch of debug stuff into the debug log file.
  debug_logfile.write("<table border=\"1\"><tr><th>Score</th><th>Key</th><th>Username</th><th>Datetime</th><th>Person</th><th>Call</th><th>Response</th></tr>")
  for response in response_list:
    exchange = getExchange(response[0])
    info = "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (response[1], exchange[0], exchange[1], exchange[2], exchange[3], exchange[4], exchange[5])
    info = info.replace("\n","<br>")
    info = info.lower()
    for i in ngrams.keys():
      grams = ngrams[i]
      for term in grams:
        info = info.replace(term, "<font color=\"red\">" + term + "</font>")
    debug_logfile.write(info + "\n")
  debug_logfile.write("</table>")

  # Choose a response randomly, but weighted by scores.
  response_result = "Error, should not give this response. Found no responses I guess."
  total_weight = sum([x[1] for x in response_list])
  choice = total_weight * random.random()
  for response in response_list:
    if choice < response[1]:
      exchange = getExchange(response[0])
      response_result = exchange[5]
      break
    choice -= response[1]

  # Finish writing the debug log file.
  debug_logfile.write("Input: %s<br><br>\n" % user_input) 
  debug_logfile.write("Response: %s<br><br>\n" % response_result)
  debug_logfile.write("<br><br><br><br>") 

  return response_result


#
# Store an n-gram in the given storage.
#
def ngram(wordlist, start, n, storage):
  gram = " ".join([wordlist[i].lower() for i in range(start, start + n)])
  if gram not in storage:
    storage[gram] = 1
  else:
    storage[gram] += 1


#
# Look up the entry in term_scores for the given term.
# It will contain a list of exchange_ids and scores,
# as a string. 
#
def getSearchResult(term):
  c.execute("select * from term_scores where term = ?", (term,))
  values = c.fetchall()
  if values:
    return values[0]
  return None


#
# Look up an exchange in the database by exchange_id.
#
def getExchange(exchange_id):
  c.execute("select * from exchanges where id=?", (exchange_id,))
  value = c.fetchall()[0]
  return value


# Input/output loop.
while True:
  user_input = raw_input("%s: " % username)
  chat_logfile.write("%s: %s\n" % (username, user_input))

  if user_input == "exit;":
    break

  response = get_response(user_input)
  chat_logfile.write("Mishkinmash: %s\n" % response)

  print "Mishkinmash: %s" % response
