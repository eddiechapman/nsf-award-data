import sys
import multiprocessing as mp

from parse import Awards
from util.num_cpus import available_cpu_count


def check_orgs(awards, year):
    print(f'Checking year: {year}')
    for soup in awards[year]:
        if len(soup('Organization')) > 1:
            with open('multiple_orgs.txt', 'a') as f:
                f.write(f'{soup.find("AwardID").text}\n')
        if len(soup('LongName')) > 2:
            if len(soup('Directorate')) > 1:
                with open('multiple_dirs.txt', 'a') as f:
                    f.write(f'{soup.find("AwardID").text}\n')
            if len(soup('Division')) > 1:
                with open('multiple_divs.txt', 'a') as f:
                    f.write(f'{soup.find("AwardID").text}\n')


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
        pool.apply_async(check_orgs, args=(awards, year))
    pool.close()
    pool.join()
