"""
The module handle the creation and updating of the yellow pages
Inside the YP object we keep:
    1. Dataframe of dated rows from the preprocessed_df to which we add an 'estimated_range' col
    for those rows that will be updated with estimations. The estimations are lists [earliest, latest]
    2. a dict of people, for each person we keep their pid, name, roles

"""
import itertools
from helpers import get_fully_dated_rows_by_julian, filter_missing_pids

class YellowPages:
    def __init__(self, preprocessed_df):
        self.yp = {}
        self.dated_rows_df = get_fully_dated_rows_by_julian(preprocessed_df)
        self.dated_rows_df['estimated_range'] = [[] for _ in range(len(self.dated_rows_df))]
        self.dated_rows_df['Split_Julian_dates'] = self.dated_rows_df['Split_Julian_dates'].astype('int')
        dated_people_df = self.dated_rows_df.groupby('PID')
        for name, group in dated_people_df:
            self.add_person_from_group(name, group)

    def add_person_from_group(self, name, group):
        # Extract and prepare data from the group, then add to yellow pages
        from_year_att = group['Split_Julian_dates'].max() * (-1)
        to_year_att = group['Split_Julian_dates'].min() * (-1)
        from_year_est = None # Placeholder for now
        to_year_est = None # Placeholder for now
        ind_name = ''.join(group['ind.Name'].unique())  # Concatenate unique names
        roles = group['Role'].unique()
        roles = list(itertools.chain.from_iterable([i.split('\n') for i in roles]))  # Split roles by newline and flatten
        num_docs = len(group['Tablet ID'].unique())

        # Add person entry
        self.yp[name] = {
            'pid': name,    #the group name which is the PID
            'name': ind_name,
            'roles': roles,
            'attested_from_year': int(from_year_att),
            'attested_to_year': int(to_year_att),
            'attested_range': int(to_year_att) - int(from_year_att),
            'estimated_from_year': from_year_est,  # Placeholder for now
            'estimated_to_year': to_year_est,  # Placeholder for now
            'doc_count': num_docs
        }

    def update_person(self, pid, role, estimated_from, estimated_to):
        person = self.yp.get(pid)
        if not person:
            print(f"WARNING: PID {pid} not found in yellow pages.")
            return

        # Add role only if not already present
        if role not in person['roles']:
            person['roles'].append(role)

        # For BCE (stored as negative), from = more recent = larger number
        if person['estimated_from_year'] is None or estimated_from > person['estimated_from_year']:
            person['estimated_from_year'] = estimated_from

        # to = further back = smaller number
        if person['estimated_to_year'] is None or estimated_to < person['estimated_to_year']:
            person['estimated_to_year'] = estimated_to

        person['doc_count'] += 1


    def add_person(self, name, ind_name, roles, from_year_est, to_year_est):
        self.yp[name] = {
            'pid': name,  # the group name which is the PID
            'name': ind_name,
            'roles': roles,
            'attested_from_year': None,
            'attested_to_year': None,
            'attested_range': None,
            'estimated_from_year': from_year_est,  # Placeholder for now
            'estimated_to_year': to_year_est,  # Placeholder for now
            'doc_count': 1
        }

    def get_num_years(self, pid):
        return self.yp[pid]['attested_range'] if pid in self.yp and self.yp[pid].get('attested_range') is not None else float('inf')


    def get_person_data(self, name):
        return self.yp.get(name, None)

    def update_yellow_pages(self):
        # this updates both the person_dict and the dated_rows_df
        return