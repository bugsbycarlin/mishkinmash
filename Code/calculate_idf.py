#
# This program calculates the IDF value of every term in the database.
#

# CREATE TABLE idf(term VARCHAR(100) NOT NULL, idf_value FLOAT, PRIMARY KEY (term));

import sqlite3

# Start the database connection.
database_path = "../Database/search_database"
connection = sqlite3.connect(database_path)
cursor = connection.cursor()

# Grab the number of exchanges.
cursor.execute("select count(*) from exchanges")
total_exchanges = float(cursor.fetchall()[0][0])
print "There are", total_exchanges, "total exchanges."

# This is just informational.
# Grab the number of terms (all unigrams, bigrams and trigrams).
cursor.execute("select count(*) from term_counts")
total_terms = float(cursor.fetchall()[0][0])
print "There are", total_terms, "total terms."

# Grab the list of unique words. this query will take a little bit of time.
# It returns a list of tuples, so we will need to look at the [0] element of each
# to get the actual term.
cursor.execute("select distinct(term) from term_counts")
word_list = cursor.fetchall()
print "There are", len(word_list), "distinct words."

# For each word, find the number of exchanges which contain the word,
# divide to get IDF, and insert an entry into the idf table.
count = 0
for word_tuple in word_list:
  word = word_tuple[0]

  # Grab the number of exchanges which contain this word.
  cursor.execute("select count(*) from term_counts where term = ?", (word,))
  exchanges_with_word = float(cursor.fetchall()[0][0])

  # This is the inverse document frequency calculation.
  idf_value = total_exchanges / exchanges_with_word

  # Save the result into the idf table.
  cursor.execute("insert or ignore into idf (term, idf_value) values(?, ?)", (word, 0))
  cursor.execute("update idf set idf_value=? where term=?", (idf_value, word))

  # By keeping a counting variable, we can print out every thousandth action. This keeps
  # us aware of the progress of the program without overwhelming us with printouts!
  if count % 1000 == 0:
    print "Completed IDF calculation for", count, "words."

  count += 1

# Commit all these actions to the database!
connection.commit()
