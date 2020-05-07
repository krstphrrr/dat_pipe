import pandas as pd

class datReader:
    arrays = []
    header = None
    df = None

    def __init__(self, path):
        """
        prepping the multirow header
        1. reads the first 4 lines of the .dat file
        2. gets each line into an list of lists
        3. appends 16 spaces to the first list
        4. create tuples with the list of lists
        5. create a multiindex with those tuples
        6. append created header to headless df

        """
        self.arrays = []
        self.header = None
        self.df = None
        with open(path, 'r') as reader:
            all_lines = reader.readlines()
            for each_line in all_lines[:4]:
                split_line = each_line.split(",")
                self.arrays.append([each_character.replace("\"","") for each_character in split_line])

        while len(self.arrays[0])<25:
            temp_space = ''
            self.arrays[0].append(temp_space)

        tuples = list(zip(*self.arrays))

        self.header = pd.MultiIndex.from_tuples(tuples)

        self.df = pd.read_table(path, sep=",", skiprows=4, low_memory=False)

        self.df.columns = self.header
        self.getdf()

    def getdf(self):
        return self.df
