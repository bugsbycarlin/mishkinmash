
import datetime
import fnmatch
import os
import re
import sqlite3

# CREATE TABLE exchanges(id integer primary key, username varchar, 
#     datetime varchar, person varchar, call varchar, response varchar);
# leaving out context for now.
# CREATE TABLE term_counts(id integer primary key, term varchar,
#     count integer, ngram integer, exchange_id integer);
# CREATE INDEX term_counts_by_term on term_counts(term);

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
          "context": ""
        }
      )
    exchanges = new_exchanges

    #context is up to three exchanges back and up to three exchanges forward
    for i, exchange in enumerate(exchanges):
      lower_range = max(0, i - 3)
      upper_range = min(len(exchanges), i + 3)
      context = ""
      for j in range(lower_range, upper_range):
        context += exchanges[j]["call"] + " " + exchanges[j]["response"] + " "
      exchanges[i]["context"] = context

    all_exchanges += exchanges

  return all_exchanges

    # test it out
    # for i, exchange in enumerate(exchanges):
    #   print
    #   print
    #   print
    #   print i
    #   print exchange["context"]
    # exit(1)

def ngram(wordlist, start, n, value, storage):
  gram = " ".join([wordlist[i].lower() for i in range(start, start + n)])
  if gram not in storage:
    storage[gram] = value
  else:
    storage[gram] += value

def process_and_store(exchanges, c):
  print "Processing and storing %d exchanges." % len(exchanges)
  for i, exchange in enumerate(exchanges):
    grams = {
      1: {},
      2: {},
      3: {}
    }

    call_words = re.findall(word_regex, exchange["call"])
    context_words = re.findall(word_regex, exchange["context"])

    for j in range(0, len(call_words)):
      ngram(call_words, j, 1, 5, grams[1])
      if j < len(call_words) - 1:
        ngram(call_words, j, 2, 5, grams[2])
      if j < len(call_words) - 2:
        ngram(call_words, j, 3, 5, grams[3])
      
    for j in range(0, len(context_words)):
      ngram(context_words, j, 1, 1, grams[1])
      if j < len(context_words) - 1:
        ngram(context_words, j, 2, 1, grams[2])
      if j < len(context_words) - 2:
        ngram(context_words, j, 3, 1, grams[3])

    store(exchange, grams, c)

    if i % 500 == 0:
      print "Processed and stored %d exchanges." % i
    
    # if i == 3000:
    #   break


def store(exchange, grams, c):
    c.execute("insert or replace into exchanges (username, datetime, person, call, response) values(?, ?, ?, ?, ?)", \
        (exchange["username"], exchange["datetime"], exchange["person"], exchange["call"], exchange["response"]))
    exchange_id = c.lastrowid

    term_count_data = []
    for term in grams[1]:
      term_count_data.append((term, grams[1][term], 1, exchange_id))
    for term in grams[2]:
      term_count_data.append((term, grams[2][term], 2, exchange_id))
    for term in grams[3]:
      term_count_data.append((term, grams[3][term], 3, exchange_id))

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