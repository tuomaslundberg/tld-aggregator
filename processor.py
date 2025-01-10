import sys
import re
import json

# The file to be processed
input_file = sys.argv[1]
# The file where the TLD distribution is dumped as a JSON object
output_file = sys.argv[2]
# This regex attempts to match the TLD contained by the URL. Please note that
# the regex is somewhat ad-hoc, so don't rely on it in critical implementations
TLD_regex = re.compile(r'\.([a-zA-Z0-9-]+)(?:\.(?=[:\/?#]|$)|(?::(?:\d+)?|[\/?#]|$))')
# The TLD distribution is saved here
distr = {}
# Error-raising JSON lines along with their error messages are written here
error_log_file = f"{output_file}.errors"

# This implementation should in principle work with huge files, since Python
# maintains an internal file pointer, and thus the file isn't loaded in memory.
# No chunking or parallelisation is implemented, so expect long runtimes
with open(input_file, 'r')  as file:
    for line in file:
        json_object = json.loads(line)
        try:
            # Find the TLD in the JSON's URL...
            match = TLD_regex.search(json_object['u']).group(1)
            # ...and save it to the distribution
            if match not in distr:
                distr[match] = 1
            else:
                distr[match] += 1
        except Exception as e:
            # Log problematic URL and error msg to the error log
            with open(error_log_file, 'a') as error_log:
                error_log.write(f"{e}\n{line}\n")

# Save the distribution to the output file
with open(output_file, 'w') as file:
    json.dump(distr, file)
