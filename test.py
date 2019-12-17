#!/usr/bin/env python

"""
Template for creating tables and filling them
"""

__author__ = "Kim Reijntjens"

##CODE

# imports
import mysql
import mysql.connector
import os
import sys
import csv
import datetime, time


# classes
class Chromosome:
    def __init__(self, row, header):
        self.__dict__ = dict(zip(header, row))
        self.chromosome_id = 0
        self.gene_id = 0
        self.Numeric_column = 0

    def __repr__(self):
        return str(self.__dict__)


# functions
def connect_to_database(user, password, database, hostname):
    """
    make connection to database
    """
    try:
        con = mysql.connector.connect(user=user, password=password,
                                      database=database, host=hostname)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        print("gelukt")
        return con


def check_file(filename):
    """
    see if file that is given as input exists and
    if it is tab seperated
    """

    if os.path.isfile(filename):
        print("file exists")
    else:
        print("file does not exist")

    return 0


def read_datafile(filename):
    data = list(csv.reader(open(filename), delimiter='\t'))
    # for index, item in enumerate(data):
    #         print(item[index] )
    # if item[index].isdigit():
    #     item[index] = "_" + item[index]

    datadict = [Chromosome(i, data[0]) for i in data[1:]]

    return datadict


def read_sql_script(cnx, filename):
    """
    reads sql scrip to make tables and puts them in the database
    """
    cursor = cnx.cursor()

    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        try:
            if command.rstrip() != '':
                cursor.execute(command)
        except ValueError as e:
            print("Command skipped: ", e)

    cnx.commit()

    return 0


def read_sql_script(con, filename):
    """
    reads sql scrip to make tables and puts them in the database
    """

    cursor = con.cursor()

    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            if command.rstrip() != '':
                cursor.execute(command)
        except ValueError as e:
            print("Command skipped: ", e)

    con.commit()
    cursor.close()

    return 0


def insert_chromsoom_data(con, datadict):
    """
    fill the tables in data with the ANNOVAR data file
    """

    cursor = con.cursor(prepared=True)
    sql_insert_query = """INSERT INTO chromosomes(CHROM) SELECT %s WHERE NOT EXISTS(Select CHROM From chromosomes WHERE CHROM = %s ) LIMIT 1"""

    for x in datadict:
        chromosoomnaam = (x.CHROM, x.CHROM)
        # print(chromosoomnaam)
        cursor.execute(sql_insert_query, (chromosoomnaam))

    con.commit()

    return 0


def update_data_chromosome_id(con, datadict):
    cursor = con.cursor()
    cursor.execute("""SELECT chromosome_id, CHROM FROM chromosomes""")
    chromosomen_list = [{'name': row[1], 'id': row[0]} for row in cursor.fetchall()]
    chromosomen_dict = {value["name"]: value["id"] for value in chromosomen_list}
    print(type(datadict), 'datadict')
    print(type(chromosomen_list), 'chromosomen_list')
    print(type(chromosomen_dict), 'chromosomen_dict')
    for x in datadict:
        x.chromosome_id = chromosomen_dict.get(x.CHROM)
        # print (x.chromosome_id,x.CHROM)
    return datadict


def insert_gene_data(con, datadict):
    """
    fill the tables in data with the ANNOVAR data file
    """

    cursor = con.cursor(prepared=True)
    sql_insert_query = """INSERT INTO genes(RefSeq_Gene, chromosome_id) SELECT %s, %s WHERE NOT EXISTS(Select * From genes WHERE RefSeq_Gene = %s ) LIMIT 1"""

    for x in datadict:
        chromosoomnaam = (x.RefSeq_Gene, x.chromosome_id, x.RefSeq_Gene)
        cursor.execute(sql_insert_query, (chromosoomnaam))

    con.commit()

    return 0


def update_data_gene_id(con, datadict):
    cursor = con.cursor()
    cursor.execute("""SELECT gene_id, RefSeq_Gene FROM genes""")
    gene_list = [{'name': row[1], 'id': row[0]} for row in cursor.fetchall()]
    gene_dict = {value["name"]: value["id"] for value in gene_list}

    for x in datadict:
        x.gene_id = gene_dict.get(x.RefSeq_Gene)

    return datadict


def insert_variants_data(con, datadict):
    cursor = con.cursor(prepared=True)
    sql_insert_query = """INSERT INTO variants(gene_id, POS, reference, observed, RefSeq_Func, dbsnp138, _1000g2015aug_EUR, LJB2_SIFT, LJB2_PolyPhen2_HDIV, LJB2_PolyPhen2_HVAR, CLINVAR)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    for x in datadict:
        variant_values = (
        x.gene_id, x.POS, x.reference, x.observed, x.RefSeq_Func, x.dbsnp138, x._1000g2015aug_EUR, x.LJB2_SIFT,
        x.LJB2_PolyPhen2_HDIV, x.LJB2_PolyPhen2_HVAR, x.CLINVAR)
        cursor.execute(sql_insert_query, (variant_values))

    con.commit()

    return 0


# main
def main(arguments):
    con = connect_to_database('root', 'Jankim00', 'kimsdatabase', 'localhost')

    # check_file("ANNOVAR.tabular")

    read_sql_script(con, 'C:/Users/Hubert/Documents/Kim/create tables.sql')

    datadict = read_datafile("C:/Users/Hubert/Documents/Kim/ANNOVAR.tabular")

    insert_chromsoom_data(con, datadict)
    datadict = update_data_chromosome_id(con, datadict)
    insert_gene_data(con, datadict)
    datadict = update_data_gene_id(con, datadict)
    insert_variants_data(con, datadict)

    # for x in datadict:
    #     print(x.gene_id, x.POS, x.reference, x.observed, x.RefSeq_Func, x.dbsnp138, x._1000g2015aug_EUR, x.LJB2_SIFT, x.LJB2_PolyPhen2_HDIV, x.LJB2_PolyPhen2_HVAR,x.CLINVAR)

    return 0


def main_ORGINEEL(arguments):
    # parser = argparse.ArgumentParser(description="create table and fill with inputfile")
    # parser.add_argument("user", type=str, help="username")
    # parser.add_argument("password", type=str, help="password")
    # parser.add_argument("database", type=str, help="database name")
    # parser.add_argument("datafilename", type=str, help="tabular seperated data")
    # args = parser.parse_args()

    # if connect_to_database('root', 'Jankim00', 'kimsdatabase'):
    #     if check_file(args.datafilename):
    #         create_tables('create_table.sql')
    #         fill_tables(args.datafilename)
    #         print("complete")
    #     else:
    #         print("failed")
    # else:
    #     print("failed")

    con = connect_to_database('root', 'Jankim00', 'kimsdatabase', 'localhost')

    # check_file("ANNOVAR.tabular")

    # read_sql_script(con,'C:/Users/Hubert/Documents/Kim/create tables.sql')
    #
    # datadict = read_datafile("C:/Users/Hubert/Documents/Kim/ANNOVAR.tabular")
    #
    # insert_chromsoom_data(con, datadict)

    read_chromosoom_table(con)

    # for x in datadict:
    #     x.set_chromosome_id(1)
    #
    # for x in datadict:
    #     print(x.chromosome_id)

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
