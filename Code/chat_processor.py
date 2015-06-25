#
# Copyright 2015, Matthew Carlin
#
# Released under MIT license. Do what you want with this code; I would appreciate if you credit me!
#
# This program reads a set of chat files (created with https://github.com/coandco/gtalk_export)
# and uses them to create a search database of chat exchanges. It is intended that this database
# be used by the accompanying chat_program.py to make a chatbot.
#

import datetime
import fnmatch
import heapq
import math
import os
import re
import sqlite3

# Prerequisites:

# 1. A ../ChatHistory directory with chat files created by gtalk_export.
# The files should have the form username_number.txt or just username.txt.

# A ../Database/search_database sqlite db file with the following tables:
# CREATE TABLE exchanges(id integer primary key, username varchar, 
#     datetime varchar, person varchar, call varchar, response varchar);
# CREATE TABLE term_scores(id integer primary key, term varchar,
#     scorelist varchar);
# CREATE INDEX term_scores_by_term on term_scores(term);

# The score of an exchange for a given term is term_count * n * idf_value / sqrt(text_length)
# n is 1 for unigrams, 2 for bigrams.
# text_length is the length in words of a block of text
# idf_value is the term's inverse document frequency, or, one over the proportion of documents it appears in

# Doesn't handle multiple line chats, of which there are about 13,000.
chat_regex = re.compile("([\d]{4}-[\d]{2}-[\d]{2}) ([\d]{2}:[\d]{2}:[\d]{2}) <([^>]*)> ([^\n]*)")
word_regex = re.compile("[\w']+|[?]")


#
# Load the conversations from the ChatHistory folder
#
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

          # a conversation key is user plus datestamp, eg "Keith 2012-02-29"
          conversation_key = "%s %s" % (username, datestamp)

          if conversation_key not in conversations:
            conversations[conversation_key] = []

          conversations[conversation_key] += [[timestamp, person, text]]

  return conversations


#
# Chunk the conversations up into exchanges. An exchange is a set of
# lines by participant A followed by a set of lines by participant B.
#
# So the following conversation gets chunked up into three exchanges:
#
# A: blah
# A: blah blah
# B: bler
# B: bler bler
# A: yar yar yar
# A: :-)
# A: hoooey
# B: blah
# B: blue blee blah
#
def get_exchanges(conversations):
  exchanges = []
  for conversation_key in conversations.keys():
    conversation = conversations[conversation_key]

    username, datestamp = conversation_key.split(" ")

    # a block is many lines in a row by one person
    single_user_blocks = []
    current_person = None

    # keep adding lines to the last block until a new person speaks,
    # then make a new block.
    for i, line in enumerate(conversation):
      person = line[1]
      if person != current_person:
        current_person = person
        single_user_blocks.append([])
      single_user_blocks[-1].append(line)

    # each exchange is a sequence of two blocks (yours then mine)
    # plus some metadata
    for i in range(0, len(single_user_blocks) - 1):
      person = single_user_blocks[i][0][1]

      # this program is hard coded to look for MEEEEEE
      if person == "bugsby.carlin@gmail.com" or person == "Matthew Carlin":
        person = "Matthew"
      else:
        person = username

      exchanges.append(
        {
          "username": username,
          "datetime": datestamp + " " + single_user_blocks[i][0][0],  #for debugging purposes later
          "person": person,
          "call": "\n".join([x[2] for x in single_user_blocks[i]]),
          "response": "\n".join([x[2] for x in single_user_blocks[i+1]]),
        }
      )

  return exchanges


#
# Store all the exchanges in the database.
#
def store_exchanges(exchanges, c, connection):
  print "Storing %d exchanges." % len(exchanges)
  for i, exchange in enumerate(exchanges):
    storeExchange(exchange, c)

    if i % 500 == 0:
      print "Stored %d exchanges." % i
      
  connection.commit()


#
# Process the exchanges, build
#
def get_and_store_term_scores(exchanges, c, connection):
  print "First pass processing %d exchanges for term scores." % len(exchanges)

  # Used to calculate IDF score.
  document_frequency = {
    1: {},
    2: {}
  }

  # Store the 50 best matches for any given term (to keep the database small).
  best_matches = {
    1: {},
    2: {}
  }

  for i, exchange in enumerate(exchanges):

    ngrams = {
      1: {},
      2: {}
    }

    # get all the words in the first block of the exchange
    call_words = re.findall(word_regex, exchange["call"])

    # square root of the length of the first block
    sq_exchange_len = math.sqrt(float(len(call_words)))

    # put ngrams into the ngrams lists
    for j in range(0, len(call_words)):
      ngram(call_words, j, 1, ngrams[1])
      if j < len(call_words) - 1:
        ngram(call_words, j, 2, ngrams[2])

    # for every ngram
    for key in ngrams.keys():
      gram = ngrams[key]
      for word in gram:

        # count it up for total document frequency
        if word not in document_frequency[key]:
          document_frequency[key][word] = gram[word]
        else:
          document_frequency[key][word] += gram[word]

        # keep best match by count, using a heapq data structure.
        if word not in best_matches[key]:
          best_matches[key][word] = []
        heapq.heappush(best_matches[key][word], [gram[word] / sq_exchange_len, exchange["exchange_id"]])
        if len(best_matches[key][word]) > 50:
          heapq.heappop(best_matches[key][word])

    if i % 500 == 0:
      print "First pass processed %d exchanges for term scores." % i

  # count up how many total words there are.
  total = 0
  for key in best_matches:
    total += len(best_matches[key])

  # finish calculating document frequency values by dividing by the total number of words. 
  for key in document_frequency:
    total_words = 0
    for word in document_frequency[key]:
      total_words += document_frequency[key][word]
    total_words = float(total_words)
    for word in document_frequency[key]:
      document_frequency[key][word] /= total_words


  # second pass: calculate scores and store them.
  print "Second pass scoring and storing %d term scores." % total

  i = 0
  for key in best_matches:
    for word in best_matches[key].keys():
      scorelist = "_".join(["%s,%0.2f" % (z[1], z[0] / document_frequency[key][word] * key) for z in best_matches[key][word]])
      storeTermScore(word, scorelist, c)

      i += 1

      if i % 5000 == 0:
        print "Second pass calculated and stored %d term scores." % i
        connection.commit()


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
# Store a term score in the term_scores table.
#
def storeTermScore(term, scorelist, c):
  c.execute("insert or replace into term_scores (term, scorelist) values(?, ?)", (term, scorelist))


#
# Store an exchange in the exchanges table.
#
def storeExchange(exchange, c):
  c.execute("insert or replace into exchanges (username, datetime, person, call, response) values(?, ?, ?, ?, ?)", \
        (exchange["username"], exchange["datetime"], exchange["person"], exchange["call"], exchange["response"]))
  exchange_id = c.lastrowid
  exchange["exchange_id"] = exchange_id


#
# Legacy method to write out conversations to individual files by conversation key.
# These are useful for debug purposes and just in general for playing with your old
# conversations!
#
def write_all_the_conversations():
  print "Writing %d conversations." % len(conversations)
  for conversation_key in conversations.keys():
    conversation = conversations[conversation_key]
    conversation.sort(key = lambda x: datetime.datetime.strptime(x[0], "%H:%M:%S"))
    outfile_name = conversation_key.replace(":","_").replace(" ","_").replace("-","_") + ".txt"

    with open("../Conversations/" + outfile_name, "w") as outfile:
      for line in conversation:
        outfile.write("%s <%s> %s\n" % tuple(line))


#
# Main program
#
if __name__ == "__main__":

  database_path = "../Database/search_database"
  connection = sqlite3.connect(database_path)
  c = connection.cursor()

  print "Getting conversations..."
  conversations = get_conversations()
  print "Getting exchanges..."
  exchanges = get_exchanges(conversations)
  print "Storing exchanges..."
  store_exchanges(exchanges, c, connection)
  print "Getting and storing term scores..."
  get_and_store_term_scores(exchanges, c, connection)
  print "All done!"