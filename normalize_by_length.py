#
# This program normalizes the term scores by conversation length, a thing I forgot to do.
# It can't ever be really accurate, because I blended all the calls, responses and context
# scores and can't go back and individually normalize these without redoing the whole
# database from scratch. But this should at least mitigate the problem.
#

import sqlite3

print "Loading words..."
word_list = []
with open("../Database/unique_words.txt", "r") as wordfile:
  for line in wordfile.lines:
    word = line.strip()
    word_list.append(word)
print "There are", len(word_list), "distinct words."

# Start the database connection.
database_path = "../Database/search_database"
connection = sqlite3.connect(database_path)
cursor = connection.cursor()


# For each word, we want to look up the length of the exchange and divide the word score by it.
# Dot dot dot...
# Shit. The word scores are ints, not floats. I am dumb, dumb as hell. Ah man, that is annoying.
# Oh well, I can do it by scores in the chat program on the fly maybe.
progress = 0
for word_tuple in word_list:
  word = word_tuple[0]
    
  cursor.execute("select * from term_counts where term = ? order by count desc limit 30", (word,))
  low_count = int(cursor.fetchall()[-1][2])

  cursor.execute("delete from term_counts where term = ? and count < ?", (word, low_count))

  if progress % 500 == 0:
    print "Completed culling for", progress, "words."

  progress += 1

  connection.commit()