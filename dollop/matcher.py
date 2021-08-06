import pandas as pd


class Match():

    def __init__(self):
        self.people = pd.read_csv('pipeline_data/csv_data/people.csv')
        self.organizations = pd.read_csv('pipeline_data/csv_data/organizations.csv')
        self.cleaned_organization = None
        self.cleaned_people = None

    def clean_people(self):
        df = self.people[['uid', 'first_name', 'last_name', 'full_name']]
        self.cleaned_people = df
    
    def clean_organization(self):
        df = self.organizations[['uid', 'name_es', 'name_en']]
        self.cleaned_organization = df

    def search(self, entity, ner_type):
        if ner_type == 'PER':
            found = self.cleaned_people[ 
                    (self.cleaned_people['uid'] == entity)|
                    (self.cleaned_people['first_name'] == entity)|
                    (self.cleaned_people['last_name'] == entity)|
                    (self.cleaned_people['full_name'] == entity)
                    ]
        else:
            found = self.cleaned_organization[ 
                    (self.cleaned_organization['uid'] == entity)|
                    (self.cleaned_organization['name_es'] == entity)|
                    (self.cleaned_organization['name_en'] == entity)
                    ]
        return not found.empty, found.values.tolist()

    def search_org(self, entity):
        found = self.cleaned_people[ 
                (self.cleaned_people['uid'] == entity)|
                (self.cleaned_people['first_name'] == entity)|
                (self.cleaned_people['last_name'] == entity)|
                (self.cleaned_people['full_name'] == entity)
                ]
        if found.empty:
            found = self.cleaned_organization[ 
                    (self.cleaned_organization['uid'] == entity)|
                    (self.cleaned_organization['name_es'] == entity)|
                    (self.cleaned_organization['name_en'] == entity)
                    ]
        return not found.empty, found.values.tolist()

    def valid_entity(self, entity_org, entity_1, type_entity):
        eorg_found = self.search_org(entity_org)
        eorg_posible = eorg_found[1][0] if eorg_found[0] else [entity_org]
        e_found = self.search(entity_1, type_entity)
        e1_possible = e_found[1][0] if e_found[0] else [entity_1]
        e_status = False
        for e1 in e1_possible:
            for e2 in eorg_posible:
                if e1 in e2:
                    e_status = True
                    break
            if e_status:
                break
        return e_status
