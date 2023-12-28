import os
import sys
import ast

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

    # Output to sdfs_dest_filenam in EXE Directory
    with open(f"{FTP_DIRECTORY}/{EXE_DIRECTORY}/.{sdfs_dest_filename}", 'w') as outfile:
        for file_name in file_names:
            # Determine to read local cached sfds file or local file
            if not os.path.isfile(f"{FTP_DIRECTORY}/{file_name}"):
                file_name = f"_{sdfs_prefix}_{file_name.split('_')[-1]}"

            # Open SDFS prefix_K files 
            with open(f"{FTP_DIRECTORY}/{file_name}", 'r') as key_file:
                key_value_pairs = key_file.readlines()

                # Combine all lines to an output file 
                outstring = ""
                for key_value_pair in key_value_pairs:
                    # Process key values pair
                    key_value_pair = key_value_pair.split('*')

                    if len(key_value_pair) < 2:
                        continue

                    key = key_value_pair[0]
                    outstring += key_value_pair[1]

            # Output key value pair
            outfile.write(outstring)

if __name__ == "__main__":
    # Grab arguments
    keys = ast.literal_eval(sys.argv[1])
    sdfs_prefix = sys.argv[2]
    sdfs_dest_filename = sys.argv[3]

    # Execute task and return
    juice_task(keys, sdfs_prefix, sdfs_dest_filename)
    