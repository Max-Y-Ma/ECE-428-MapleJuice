import os
import re
import sys

"""
    MapleJuice Filter SQL Query:

    Dataset: CSVs

    Maple Phase:
        - For each line in CSV, output KV pair if matches regex:
            - <1, line>

    Juice Phase:
        - Take file with key 1, sum up all lines:
            - <1, concate(line))>
"""

"""
    Maple Framework:

    Inputs:
        - Input file containing data
        - SDFS filename prefix

    Outputs: 
        - Files containing KV pairs named: f"{sdfs_prefix}_{key}"
        - Stdout all keys generated
"""

""" Maple Framework Code Start """

FTP_DIRECTORY = f"{os.environ.get('PWD')}/data"
EXE_DIRECTORY = "bin"

def key_value_pair_formatter(key, value):
    """ Format key value pair with delimiter"""
    return f"{key}*{value}\n"

def maple_filter(input_string):
    """
    Remove special characters from the input string.

    Parameters:
    - input_string (str): The input string from which characters will be removed.
    - characters_to_remove (str): A string containing the characters to be removed.

    Returns:
    - str: The input string with specified characters removed.
    """
    result = ""
    special_characters = [
        '!', '"', '#', '$', '%', '&', "'", '(', ')', '*',
        '+', ',', '-', '.', '/', ':', ';', '<', '=', '>',
        '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|',
        '}', '~', '\n', ' '
    ]
    for char in input_string:
        if char not in special_characters:
            result += char
    return result

def maple_output(keys_list):
    """
    Prints generated keys to stdout. This is the output
    format of the maple framework.
    """
    for n in range(len(keys_list)):
        if n == (len(keys_list) - 1):
            print(f"{keys_list[n]}", end="")
        else:
            print(f"{keys_list[n]}", end=" ")

""" Maple Framework Code End """

def maple_task(infile, prefix, regex):
    """
    Returns a list of keys from a given input file
    """

    output_file_prefix = f".{prefix}"
    keys_list = []

    # Open the file in read mode ('r')
    with open(infile, 'r') as csvfile:

        # Universal key for lines matching regular expression
        key = "1"

        # Output file path
        file_path = f"{FTP_DIRECTORY}/{EXE_DIRECTORY}/{output_file_prefix}_{key}"
        
        # Split the file into lines
        lines_list = csvfile.readlines()

        # Check if line matches regular expression
        for line in lines_list:

            # Verify regular expression
            if re.search(regex, line):

                if not os.path.isfile(file_path):
                    # Open the file in write mode ('w') if it doesn't exist
                    with open(file_path, 'w') as wfile:
                        # Write content to the file
                        wfile.write(key_value_pair_formatter(key, line))

                    keys_list.append(key)
                else:
                    # Open the file in append mode ('a')
                    with open(file_path, 'a') as afile:
                        # Append content to the file
                        afile.write(key_value_pair_formatter(key, line))

    return keys_list

if __name__ == "__main__":
    # Grab arguments
    input_file = sys.argv[1]
    sdfs_prefix = sys.argv[2]
    regex = sys.argv[3]

    # Execute task and return
    out_keys = maple_task(input_file, sdfs_prefix, regex)
    maple_output(out_keys)
