import sys
import argparse


def get_databse_login_args(args):
    if len(args) - 1 == 4:
        parser = argparse.ArgumentParser(description="Get database login variables")
        parser.add_argument("user", type=str, help="username")
        parser.add_argument("password", type=str, help="password")
        parser.add_argument("database", type=str, help="database name")
        parser.add_argument("host", type=str, help="hostname")
        args = parser.parse_args()
        return args
    else:
        print("Incorrect number of arguments specified. expected 4 but recieved ", len(args) - 1)
        sys.exit()


def main(args):
    args = get_databse_login_args(args)

    print(args.host)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
