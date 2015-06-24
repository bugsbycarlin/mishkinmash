
import datetime
import fnmatch
import heapq
import math
import os
import re
import sqlite3

# CREATE TABLE exchanges(id integer primary key, username varchar, 
#     datetime varchar, person varchar, call varchar, response varchar);
# CREATE TABLE term_scores(id integer primary key, term varchar,
#     scorelist varchar);
# CREATE INDEX term_scores_by_term on term_scores(term);

# Score is term_count * n * idf_value / text_length, where n is 1 for unigrams, 2 for bigrams.

# Doesn't handle multiple line chats, of which there are about 13,000.
chat_regex = re.compile("([\d]{4}-[\d]{2}-[\d]{2}) ([\d]{2}:[\d]{2}:[\d]{2}) <([^>]*)> ([^\n]*)")
word_regex = re.compile("[\w']+|[?]")

def get_conversations():

  chat_files_directory = os.listdir("../ChatHistory")
  chat_files = [x for x in chat_files_directory if fnmatch.fnmatch(x, "*.txt")]

  # a conversation takes place with a user on a particular day, and is sorted by timestamp
  conversations = {}

  for filename in chat_files:

    username = filename.replace(".txt", "").split("_")[0]

    print "Processing %s" % filename

    with open("../ChatHistory/" + filename, "r") as chatfile:
      lines = chatfile.readlines()

      for line in lines:
        match = chat_regex.match(line)
        if match:        
          datestamp = match.groups()[0] #%Y-%m-%d
          timestamp = match.groups()[1] #%H:%M:%S
          person = match.groups()[2]
          text = match.groups()[3]

          conversation_key = "%s %s" % (username, datestamp)

          if conversation_key not in conversations:
            conversations[conversation_key] = []

          conversations[conversation_key] += [[timestamp, person, text]]

  return conversations

def get_exchanges(conversations):
  all_exchanges = []
  for conversation_key in conversations.keys():
    conversation = conversations[conversation_key]

    username, datestamp = conversation_key.split(" ")

    exchanges = []
    current_person = None

    for i, line in enumerate(conversation):
      person = line[1]
      if person != current_person:
        current_person = person
        exchanges.append([])
      exchanges[-1].append(line)

    new_exchanges = []
    for i in range(0, len(exchanges) - 1):
      person = exchanges[i][0][1]

      if person == "bugsby.carlin@gmail.com" or person == "Matthew Carlin":
        person = "Matthew"
      else:
        person = username

      new_exchanges.append(
        {
          "username": username,
          "datetime": datestamp + " " + exchanges[i][0][0],  #for debugging purposes later
          "person": person,
          "call": "\n".join([x[2] for x in exchanges[i]]),
          "response": "\n".join([x[2] for x in exchanges[i+1]]),
        }
      )
    exchanges = new_exchanges

    all_exchanges += exchanges

  return all_exchanges

def ngram(wordlist, start, n, storage):
  gram = " ".join([wordlist[i].lower() for i in range(start, start + n)])
  if gram not in storage:
    storage[gram] = 1
  else:
    storage[gram] += 1

def process_and_store(exchanges, c):
  print "Storing %d exchanges." % len(exchanges)
  for i, exchange in enumerate(exchanges):
    storeExchange(exchange, c)

    if i % 500 == 0:
      print "Stored %d exchanges." % i
      
  connection.commit()


  print "First pass processing %d exchanges for term scores." % len(exchanges)

  document_frequency = {
    1: {},
    2: {}
  }
  best_matches = {
    1: {},
    2: {}
  }
  for i, exchange in enumerate(exchanges):
    counts = {
      1: {},
      2: {}
    }

    call_words = re.findall(word_regex, exchange["call"])
    sq_exchange_len = math.sqrt(float(len(call_words)))

    for j in range(0, len(call_words)):
      ngram(call_words, j, 1, counts[1])
      if j < len(call_words) - 1:
        ngram(call_words, j, 2, counts[2])

    for key in counts.keys():
      gram = counts[key]
      for word in gram:
        if word not in document_frequency[key]:
          document_frequency[key][word] = gram[word]
        else:
          document_frequency[key][word] += gram[word]
        if word not in best_matches[key]:
          best_matches[key][word] = []
        heapq.heappush(best_matches[key][word], [gram[word] / sq_exchange_len, exchange["exchange_id"]])
        if len(best_matches[key][word]) > 50:
          heapq.heappop(best_matches[key][word])


    # here's where we process scores. we're not going to be in the business of storing anything yet.
    
    #store(exchange, grams, c)

    if i % 500 == 0:
      print "First pass processed %d exchanges for term scores." % i

  total = 0
  for key in best_matches:
    total += len(best_matches[key])
  print "Second pass processing %d term scores." % total

  for key in document_frequency:
    total_words = 0
    for word in document_frequency[key]:
      total_words += document_frequency[key][word]
    total_words = float(total_words)
    for word in document_frequency[key]:
      document_frequency[key][word] /= total_words

  # word_ids = {}
  # i = 0
  # for key in best_matches:
  #   for word in best_matches[key].keys():
  #     if word not in word_ids:
  #       word_id = storeTerm(word, c)
  #       word_ids[word] = word_id

  #     i += 1

  #     if i % 5000 == 0:
  #       print "Storing word %d." % i
  # connection.commit()
  
  i = 0
  for key in best_matches:
    for word in best_matches[key].keys():
      scorelist = "_".join(["%s,%0.2f" % (z[1], z[0] / document_frequency[key][word] * key) for z in best_matches[key][word]])
      storeTermScore(word, scorelist, c)

      i += 1

      if i % 5000 == 0:
        print "Second pass calculated and stored %d term scores." % i
        connection.commit()


def storeTermScore(term, scorelist, c):
  c.execute("insert or replace into term_scores (term, scorelist) values(?, ?)", (term, scorelist))

# def storeTerm(term, c):
#   c.execute("insert or replace into terms (term) values(?)", (term, ))
#   term_id = c.lastrowid
#   return term_id

def storeExchange(exchange, c):
  c.execute("insert or replace into exchanges (username, datetime, person, call, response) values(?, ?, ?, ?, ?)", \
        (exchange["username"], exchange["datetime"], exchange["person"], exchange["call"], exchange["response"]))
  exchange_id = c.lastrowid
  exchange["exchange_id"] = exchange_id

def store(exchange, grams, c):
    c.execute("insert or replace into exchanges (username, datetime, person, call, response) values(?, ?, ?, ?, ?)", \
        (exchange["username"], exchange["datetime"], exchange["person"], exchange["call"], exchange["response"]))
    exchange_id = c.lastrowid

    term_count_data = []
    for term in grams[1]:
      term_count_data.append((term, grams[1][term], 1, exchange_id))
    for term in grams[2]:
      term_count_data.append((term, grams[2][term], 2, exchange_id))

    c.executemany("insert or replace into term_counts (term, count, ngram, exchange_id) values(?, ?, ?, ?)", (term_count_data))

    connection.commit()


def write_all_the_conversations():
  print "Writing %d conversations." % len(conversations)
  for conversation_key in conversations.keys():
    conversation = conversations[conversation_key]
    conversation.sort(key = lambda x: datetime.datetime.strptime(x[0], "%H:%M:%S"))
    outfile_name = conversation_key.replace(":","_").replace(" ","_").replace("-","_") + ".txt"

    with open("../Conversations/" + outfile_name, "w") as outfile:
      for line in conversation:
        outfile.write("%s <%s> %s\n" % tuple(line))

if __name__ == "__main__":

  database_path = "../Database/search_database"
  connection = sqlite3.connect(database_path)
  c = connection.cursor()

  print "Getting conversations..."
  conversations = get_conversations()
  print "Getting exchanges..."
  exchanges = get_exchanges(conversations)
  print "Processing and storing exchanges..."
  process_and_store(exchanges, c)
  print "All done!"