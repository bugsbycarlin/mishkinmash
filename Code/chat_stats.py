
import fnmatch
import os
import re

# this won't work because there are multiple newline lines. tricky.
# fricky tricky. maybe i break up by timestamp and just deal with the
# consequences in a few edge cases. le sigh, programming really is hard.
chat_regex = re.compile("([\d]{4}-[\d]{2}-[\d]{2} [\d]{2}:[\d]{2}:[\d]{2}) <([^>]*)> ([^\n]*)")

chat_files_directory = os.listdir("../ChatHistory")
chat_files = [x for x in chat_files_directory if fnmatch.fnmatch(x, "*.txt")]

lines = {}
no_match = 0

for filename in chat_files:

  username = filename.replace(".txt", "")
  username = username.split("_")[0]

  print "Processing %s" % filename

  if not username in lines:
    lines[username] = [0, 0]

  with open("../ChatHistory/" + filename, "r") as f:
    current_person = None
    current_exchange = []

    for line in f.readlines():
      match = chat_regex.match(line)
      if match:
        lines[username][0] += 1
        
        timestamp = match.groups()[0]
        person = match.groups()[1]
        line = match.groups()[2]

        if person == "bugsby.carlin@gmail.com" or person == "Matthew Carlin":
          lines[username][1] += 1
      else:
        no_match += 1

print "I have chat history with %d people." % len(lines)

l = lines.keys()
l.sort()
for user in l:
  me_percent = lines[user][1] * 100.0 / (1.0 * lines[user][0])
  me_string = "I listen more"
  if me_percent > 50.0:
    me_string = "I talk more"
  print "%s: %d lines with %d by me, %0.2f. %s." % \
      (user, lines[user][0], lines[user][1], me_percent, me_string)

print "Number of failed match lines: %d." % no_match