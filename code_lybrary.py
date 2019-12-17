# Open and read the file as a single buffer
with open("filename, 'r') as fd:
sqlFile = fd.read()


def read_datafile(filename):
    data = list(csv.reader(open(filename), delimiter='\t'))
    datadict = [Chromosome(i, data[0]) for i in data[1:]]

    return datadict
