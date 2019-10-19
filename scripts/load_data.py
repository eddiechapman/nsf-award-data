"""
load_data.py

Parse a directory of zip files containing NSF awards and insert them into a database.
"""
import logging
import sys
import db
from awards import AwardExplorer
from parse import parse_award


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.propagate = False
    f_handler = logging.FileHandler('load_data.log', mode='w')
    f_handler.setLevel(logging.INFO)
    f_formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(message)s')
    f_handler.setFormatter(f_formatter)
    logger.addHandler(f_handler)

    logger.info('Parsing award data.')

    try:
        awards = AwardExplorer(sys.argv[1])
    except IndexError:
        logger.error(f'No zip directory argument specified.')
        print(f'{sys.argv[0]} <zipdir>')
        sys.exit(1)

    logger.info(f'Award years: {sorted(awards.years())}')

    for year in sorted(awards.years()):
        logger.debug(f'Beginning award year {year}')
        awardgen = awards[year]
        session = db.Session()

        for i, award in enumerate(awardgen):
            try:
                logger.debug(f'Inserting award {award.id}')
                parse_award(award, session)
            except Exception as e:
                logger.error(f'Error in award {award.id}: {e}')
                session.rollback()

        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f'ROLLBACK - Error in year {year}. {e}')
