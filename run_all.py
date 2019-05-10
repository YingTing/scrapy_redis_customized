import argparse
import logging

from run_list_files import list_files
from run_check_and_push_new_files import check_and_push_new_files
from run_show_status import show_status

logger = logging.getLogger(__name__)
mylog = print

def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_list = subparsers.add_parser('list-files', help='list-files')
    parser_list.add_argument('-m', '--mall_name', default="all", nargs='+')
    parser.add_argument('-p', '--property', default='buy', dest='mall_property')
    parser.add_argument('-f', '--full', type=bool, default=0)
    parser_list.set_defaults(func=list_files)

    parser_push = subparsers.add_parser('check', help='check (and push) new files')
    parser_push.add_argument('-s', '--redis_host', default='localhost')
    parser_push.add_argument('-p', '--redis_port', default=6379, type=int)
    parser_push.add_argument('--redis_db', default=0, type=int)
    parser_push.add_argument('--repeat', default=0, type=int)
    parser_push.add_argument('--clean_key', action='store_true', help='clean redis key first')
    parser_push.add_argument('--push', action='store_true', help='push to redis')
    parser_push.add_argument('-t', '--datatime_after', required=True)
    parser_push.add_argument('-m', '--mall_name', required=True, nargs='+')
    parser_push.set_defaults(func=check_and_push_new_files)

    parser_status = subparsers.add_parser('status', help='show status')
    parser_status.add_argument('-s', '--redis_host', default='localhost')
    parser_status.add_argument('-p', '--redis_port', default=6379, type=int)
    parser_status.add_argument('--redis_db', default=0, type=int)
    parser_status.add_argument('-m', '--mall_name', default="all", nargs='+')
    parser_status.set_defaults(func=show_status)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
