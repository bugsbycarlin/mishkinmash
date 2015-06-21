#
# This program cleans not so useful results from the database
#

import sqlite3

# Start the database connection.
database_path = "../Database/search_database"
connection = sqlite3.connect(database_path)
cursor = connection.cursor()

# until you start using the unique word list
exit(1)

# Grab the list of unique words. this query will take a little bit of time.
# It returns a list of tuples, so we will need to look at the [0] element of each
# to get the actual term.
cursor.execute("select distinct(term) from term_counts")
word_list = cursor.fetchall()
print "There are", len(word_list), "distinct words."

with open("../Database/unique_words.txt", "w") as wordfile:
  for word_tuple in word_list:
    word = word_tuple[0]
    wordfile.write(word + "\n")
print "Wrote words to Database/unique_words.txt. Stop using the database to find the list of unique words!"

# For each word, we want to preserve about 30 hits. So look for the top 30
# results, then read the value of the last result, and then delete everything
# with a count below that value.
# eg select * from term_counts where term=<word> order by count desc limit 30;
# find the lowest number from the result, then
# delete from term_counts where term=<word> and count < <low_count>;
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