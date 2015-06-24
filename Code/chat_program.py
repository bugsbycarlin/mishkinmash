
import datetime
import math
import random
import re
import sqlite3
import sys

word_regex = re.compile("[\w']+|[?]")

session_id = str(datetime.datetime.now()).replace(" ","_").replace(":","_").split(".")[0]
print "Your session session_id is %s. Your log files are in ../Logs." % session_id

chat_logfile = open("../Logs/%s.txt" % session_id, "w")
debug_logfile = open("../Logs/%s_debug.html" % session_id, "w")
debug_logfile.write("<html><body>\n")

database_path = "../Database/search_database"
connection = sqlite3.connect(database_path)
c = connection.cursor()

username = "User"
if len(sys.argv) > 1:
  username = sys.argv[1]

def getResult(term):
  """Get term counts and url_ids for all pages matching a term."""

  c.execute("select * from term_scores where term = ?", (term,))
  values = c.fetchall()
  if values:
    return values[0]
  return None

def getExchange(exchange_id):
  """Get the exchange for a given exchange_id."""

  c.execute("select * from exchanges where id=?", (exchange_id,))
  value = c.fetchall()[0]
  return value

def ngram(wordlist, start, n, storage):
  gram = " ".join([wordlist[i].lower() for i in range(start, start + n)])
  if gram not in storage:
    storage[gram] = 1
  else:
    storage[gram] += 1

def get_response(user_input):
  words = re.findall(word_regex, user_input)
  words = [w.lower() for w in words]

  ngrams = {
    1: {},
    2: {}
  }

  for j in range(0, len(words)):
    ngram(words, j, 1, ngrams[1])
    if j < len(words) - 1:
      ngram(words, j, 2, ngrams[2])

  results = {}

  for i in ngrams.keys():
    grams = ngrams[i]
    for term in grams:
      if term not in results:
        result = getResult(term)
        if result:
          results[term] = [[int(pairstring.split(",")[0]), float(pairstring.split(",")[1])] for pairstring in result[2].split("_")]
     
  responses = {} 
  for term in results.keys():
    for pair in results[term]:
      if pair[0] not in responses:
        responses[pair[0]] = 0
      responses[pair[0]] += pair[1]

  response_list = [[k,v] for k,v in responses.items()]
  response_list.sort(key = lambda x: x[1], reverse = True)
  if len(response_list) > 40:
    response_list = response_list[0:40]

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

  response_result = "Error, should not give this response. Found no responses I guess."
  total_weight = sum([x[1] for x in response_list])
  choice = total_weight * random.random()
  for response in response_list:
    if choice < response[1]:
      exchange = getExchange(response[0])
      response_result = exchange[5]
      break
    choice -= response[1]

  debug_logfile.write("Input: %s<br><br>\n" % user_input) 
  debug_logfile.write("Response: %s<br><br>\n" % response_result)
  debug_logfile.write("<br><br><br><br>") 


  return response_result


while True:
  user_input = raw_input("%s: " % username)
  chat_logfile.write("%s: %s\n" % (username, user_input))

  if user_input == "exit;":
    break

  response = get_response(user_input)
  chat_logfile.write("Mishkinmash: %s\n" % response)

  print "Mishkinmash: %s" % response