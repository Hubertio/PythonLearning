import sys
import argparse


def get_database_login_args(args):
    """
    Reads the parameters from the command prompt
    and adds them as argument to the args object
    """
    if len(args) - 1 == 4:
        parser = argparse.ArgumentParser(description="Get database login variables")
        parser.add_argument("user", type=str, help="username")
        parser.add_argument("password", type=str, help="password")
        parser.add_argument("database", type=str, help="database name")
        parser.add_argument("host", type=str, help="hostname")
        args = parser.parse_args()
        return args
    else:
        print(
            "Incorrect number of arguments for connecting to the database!. Expected 4 (username ,password, database name, hostname) but received ",
            len(args) - 1)
        sys.exit()


class Database:
    def __init__(self):
        self.connection = None
        self.students = Students

    def connect_to_database(self, user, password, database, hostname):
        """
        try to connection to the database and return the connection object
        """
        try:
            conn = mysql.connector.connect(user=user, password=password,
                                           database=database, host=hostname)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            self.connection = conn

    def fill_database(self, filename):
        """
        reads sql script and execute commands
        """

        cursor = self.connection.cursor()

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

        self.connection.commit()
        cursor.close()


# =======================for testing=============================
def main(args):
    args = get_database_login_args(args)
    print(type(args))
    print(args)

    db = Database
    db.add_argument("host", type=str, help="hostname")
    f = 'ff'


if __name__ == '__main__':
    sys.exit(main(sys.argv))
