"""
Retrieve the IDs for all awards associated with a given directorate.
"""
import argparse
import csv
import logging
import db


def main(args):
    logging.basicConfig(
        level=args.log_level or logging.INFO,
        filename='get_awards_by_directorate.log',
        format='%(levelname)s:%(asctime)s:%(message)s'
    )
    logging.info('Beginning program.')

    DIRECTORATE = args.directorate.upper()

    logging.info('Initializing database session.')
    session = db.Session()

    logging.info(f'Getting the ID for the directorate: {DIRECTORATE}')
    q = session.query(db.Directorate).filter(
        db.Directorate.name == DIRECTORATE
    )
    if q.count() == 0:
        q = session.query(db.Directorate).all()
        dir_list = '\n'.join([f'\t{d.id}\t{d.name}' for d in q])
        logging.error(f'Directorate not found in DB: {DIRECTORATE}')
        logging.error(
            'Try using one of the following IDs or names:\n' + str(dir_list)
        )
        raise SystemExit
    dir_id = q.first().id
    logging.info(f'Found directorate ID: {dir_id}.')

    logging.info('Getting division IDs for the directorate')
    q = session.query(db.Division).filter(db.Division.dir_id == dir_id)
    logging.info(f'Divisions in query: {q.count()}')
    divs = {div.id: div.code for div in q.all()}

    logging.info('Finding the program IDs for the divisions.')
    q = session.query(db.Program).filter(db.Program.div_id.in_(divs.keys()))
    logging.debug(f'Programs in query: {q.count()}')
    pgms = {pgm.id: pgm.code for pgm in q.all()}

    logging.info('Getting the IDs of awards funded by the programs.')
    q = session.query(db.Funding).filter(db.Funding.pgm_id.in_(pgms.keys()))
    logging.debug(f'Fundings in query: {q.count()}')
    funded = {fund.award_id: fund.pgm_id for fund in q.all()}

    logging.info('Getting the award info for funded awards.')
    q = session.query(db.Award).filter(db.Award.id.in_(funded.keys()))
    logging.debug(f'Awards in query: {q.count()}')
    awards = []
    for award in q.all():
        awards.append({
            'id': award.id,
            'code': award.code,
            'title': award.title,
            'abstract': award.abstract,
            'effective': award.effective,
            'program': pgms[funded[award.id]]
        })
    logging.info(f'Collected info on {len(awards)} awards.')

    logging.info(f'Opening the output file: {args.output}')
    with open(args.output, 'w') as f:
        logging.info('Writing the award info to file.')
        writer = csv.DictWriter(f, awards[0].keys())
        writer.writeheader()
        writer.writerows(awards)

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
