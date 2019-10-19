"""
load_data.py

Insert award records into a database using zipped XML files.
"""
import logging
import sys
import db
from awards import AwardExplorer
from parse import parse_award


if __name__ == '__main__':

    logging.basicConfig(
        filename='load_data.log',
        level=logging.DEBUG,
        format='%(levelname)s:%(asctime)s:%(message)s',
    )

    logging.info('Parsing award data.')

    try:
        awards = AwardExplorer(sys.argv[1])
    except IndexError:
        logging.error(f'No zip directory argument specified. {sys.argv[0]} <zipdir>')
        sys.exit(1)

    logging.info(f'Award years: {sorted(awards.years())}')

    counter = 0
    for year in [2015]:
        logging.info(f'Beginning award year {year}')
        awardgen = awards[year]
        session = db.Session()

        # This attempts to catch errors parsing XML that maybe can't be done in a for loop?
        while True:
            if counter % 1000 == 0:
                logging.info(f'Awards inserted:\t{counter}.')
            try:
                award = next(awardgen)
            except StopIteration:
                break
            except Exception as e:
                logging.error(f'Error parsing XML: {e}')
            else:
                logging.debug(f'Parsed XML for award {award.id}')
                try:
                    parse_award(award, session)
                except Exception as e:
                    logging.error(f'Error inserting record: {e}')
                else:
                    logging.debug(f'Inserted record for award {award.id}')
            counter += 1
            
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f'ROLLBACK - Error in year {year}. {e}')
        else:
            logging.info(f'Successfully commit awards for year {year}')
