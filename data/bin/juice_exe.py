import os
import sys
import ast

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
    Juice Framework:

    Inputs:
        - List of keys
        - SDFS filename prefix
        - SDFS destination filename

    Outputs: 
        - Append all output to sdfs_dest_filename
"""

""" Juice Framework Code Start """

FTP_DIRECTORY = f"{os.environ.get('PWD')}/data"
EXE_DIRECTORY = "bin"

def key_value_pair_formatter(key, value):
    """ Format key value pair with delimiter"""
    return f"{key}*{value}\n"

def juice_filter(input_string):
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

""" Juice Framework Code End """

def juice_task(keys, sdfs_prefix, sdfs_dest_filename):
    """
    Reads file sdfs_prefix_K and sums all the values for each key K.
    Writes or appends <word/number, sum(V)> to sdfs_dest_filename.
    """

    # Generate file names
    file_names = [f".{sdfs_prefix}_{key}" for key in keys]

    # Output to sdfs_dest_filename in EXE Directory
    with open(f"{FTP_DIRECTORY}/{EXE_DIRECTORY}/.{sdfs_dest_filename}", 'w') as outfile:
        for file_name in file_names:
            key = ""
            sum = 0

            # Determine to read local cached sfds file or local file
            if not os.path.isfile(f"{FTP_DIRECTORY}/{file_name}"):
                file_name = f"_{file_name}"

            # Open SDFS prefix_K files 
            with open(f"{FTP_DIRECTORY}/{file_name}", 'r') as key_file:
                key_value_pairs = key_file.readlines()

                for key_value_pair in key_value_pairs:
                    # Process key values pair
                    key_value_pair = key_value_pair.split('*')
                    key = key_value_pair[0]
                    sum += int(key_value_pair[1])

            # Output key value pair
            outfile.write(key_value_pair_formatter(key, sum))

if __name__ == "__main__":
    # Grab arguments
    keys = ast.literal_eval(sys.argv[1])
    sdfs_prefix = sys.argv[2]
    sdfs_dest_filename = sys.argv[3]

    # Execute task and return
    juice_task(keys, sdfs_prefix, sdfs_dest_filename)