import os
import sys

"""
    MapleJuice Wordcount/Numbercount example:

    Dataset: CSVs

    Maple Phase:
        - For each word/number in the CSV, output a KV pair:
            - <word/number, 1>

    Juice Phase:
        - For each key, sum up all the 1 values and output:
            - <word/number, sum(V)>
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

def maple_task(infile, prefix):
    """
    Returns a list of keys from a given input file
    """

    output_file_prefix = f".{prefix}"
    keys_list = []

    # Open the file in read mode ('r')
    with open(infile, 'r') as csvfile:

        # Split the file into words
        words_list = csvfile.read().split(',')

        for word in words_list:
            # Parse Key
            key = maple_filter(word)
            if key == "":
                continue

            # Register file_path
            file_path = f"{FTP_DIRECTORY}/{EXE_DIRECTORY}/{output_file_prefix}_{key}"
            if not os.path.isfile(file_path):
                # Open the file in write mode ('w') if it doesn't exist
                with open(file_path, 'w') as wfile:
                    # Write content to the file
                    wfile.write(key_value_pair_formatter(key, 1))

                keys_list.append(key)
            else:
                # Open the file in append mode ('a')
                with open(file_path, 'a') as afile:
                    # Append content to the file
                    afile.write(key_value_pair_formatter(key, 1))

    return keys_list

if __name__ == "__main__":
    # Grab arguments
    input_file = sys.argv[1]
    sdfs_prefix = sys.argv[2]

    # Execute task and return
    out_keys = maple_task(input_file, sdfs_prefix)
    maple_output(out_keys)
