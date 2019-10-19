import sys
import multiprocessing as mp

from parse import Awards
from util.num_cpus import available_cpu_count


def check_stuff(awards, year):
    print(f'Checking year: {year}')
    for soup in awards[year]:
        length = len(soup.find('AwardID').text)
        if length > 7:
            print(length)
            sys.stdout.flush()


if __name__ == "__main__":
    try:
        zipdir = sys.argv[1]
    except IndexError:
        print(f'{sys.argv[0]} <zipdir>')
        sys.exit(1)

    awards = Awards(zipdir)
    years = awards.years()
    print(f'Checking {len(years)} years.')
    cpus = available_cpu_count()
    pool = mp.Pool(processes=cpus)
    for year in years:
        pool.apply_async(check_stuff, args=(awards, year))
    pool.close()
    pool.join()
