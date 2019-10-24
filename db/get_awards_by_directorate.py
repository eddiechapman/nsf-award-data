"""
Retrieve the IDs for all awards associated with a given directorate.
"""
import argparse
import csv
import json
import logging
import db


def main(args):
    logging.basicConfig(
        level=args.log_level or logging.INFO,
        filename='get_awards_by_directorate.log',
        format='%(levelname)s:%(asctime)s:%(message)s'
    )
    logging.info('Beginning program.')

    logging.debug('Opening org-hierarchy.json')
    with open('../datainfo/org-hierarchy.json', 'r') as f:
        org_hierarchy = json.load(f)
        directorate = org_hierarchy.get(args.directorate)
        if not directorate:
            logging.error(f'Unknown directorate: {args.directorate}.')
            raise SystemExit

    logging.debug('Getting programs for the directorate')
    programs = []
    for division in directorate.values():
        programs.extend(pgm for pgm in division if pgm)
    logging.info(f'Number of programs found: {len(programs)}')

    logging.debug('Initializing database session.')
    session = db.Session()

    logging.debug('Querying the Funding table by program ID.')
    query = session.query(db.Funding).filter(db.Funding.pgm_id.in_(programs))
    logging.debug(f'Found {query.count()}')

    logging.debug(f'Opening the output file: {args.output}')
    with open(args.output, 'w') as f:
        writer = csv.writer(f)
        for row in query.all():
            writer.writerow((row.pgm_id, row.award_id))

    logging.info(f'Program complete. Output located at: {args.output}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '-v', '--verbose',
        help='Verbose (debug) logging level.',
        const=logging.DEBUG,
        dest='log_level',
        nargs='?',
    )
    group.add_argument(
        '-q', '--quiet',
        help='Silent mode, only log warnings and errors.',
        const=logging.WARN,
        dest='log_level',
        nargs='?',
    )
    parser.add_argument(
        '-d', '--directorate',
        required=True,
        metavar='directorate',
        type=str,
        help='The name of the nsf directorate.',
    )
    parser.add_argument(
        '-o', '--output',
        required=True,
        metavar='output',
        type=str,
        help='The path to the CSV file where the ids will be written.'
    )
    args = parser.parse_args()
    main(args)
