# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import networkx as nx
import pickle
import os.path
import mysql.connector

treatments = ['Antibiotic', 'Clinical Drug',
              'Hazardous or Poisonous Substance', 'Organic Chemical',
              'Pharmacologic Substance', 'Steroid', 'Vitamin']

diseases = ['Acquired Abnormality', 'Anatomical Abnormality',
            'Congenital Abnormality', 'Disease or Syndrome',
            'Cell or Molecular Dysfunction', 'Neoplastic Process',
            'Pathologic Function', 'Sign or Symptom']

themes = ['Anatomical Structure', 'Body Location or Region',
          'Body Part, Organ, or Organ Component', 'Body Space or Junction',
          'Cell Component', 'Cell', 'Laboratory or Test Result',
          'Biologic Function', 'Cell Function', 'Genetic Function',
          'Molecular Function', 'Organism Function',
          'Organ or Tissue Function', 'Physiologic Function',
          'Amino Acid, Peptide, or Protein', 'Enzyme', 'Hormone']

interested_rl_str = '''
may_treat, may_prevent, cause_of, causative_agent_of, contraindicated_with_disease
'''

beneficial_rl_str = '''may_treat, may_prevent'''

harmful_rl_str = '''cause_of, causative_agent_of, contraindicated_with_disease'''

# dict of cui -> (name, st set, st type vec)
interested_concepts = {}

# dict of cui -> (name, st set)
concept_map = {}

# dict of name -> (cui, st set)
concept_name_map = {}

# map of st id to its name
st_id_name = {}

interested_st = treatments + diseases + themes

interested_rl = interested_rl_str.split(',')
interested_rl = map(lambda s: s.strip(), interested_rl)

beneficial_rl = map(lambda s: s.strip(), beneficial_rl_str.split(','))

harmful_rl = map(lambda s: s.strip(), harmful_rl_str.split(','))

rl_map = {}
for rl in beneficial_rl:
    rl_map[rl] = 'beneficial'
for rl in harmful_rl:
    rl_map[rl] = 'harmful'

inverse = {}

def compute_concepts_map(name_only = False):
    '''
  go through MRCONSO, create a dict with cui to name map
  go through mrst, update the set
  '''
    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)

    global concept_map
    concepts_query = 'select cui, str from MRCONSO'
    concept_cursor = cnx.cursor()
    concept_cursor.execute(concepts_query)
    for (cui, name) in concept_cursor:
        cui = str(cui)
        name = str(name).lower()
        concept_map[cui] = (name, set())
    concept_cursor.close()

    if name_only:
        pickle.dump(concept_map, open("concepts.pickle", "wb"))

    global st_id_name
    srdef_query = 'select ui, sty_rl from SRDEF'
    srdef_cursor = cnx.cursor()
    srdef_cursor.execute(srdef_query)
    for (id, name) in srdef_cursor:
        id = str(id)
        name = str(name).encode('ascii', 'ignore')
        st_id_name[id] = name
    srdef_cursor.close()

    mrsty_query = 'select cui, tui from MRSTY'
    mrsty_cursor = cnx.cursor()
    mrsty_cursor.execute(mrsty_query)
    for (cui, tui) in mrsty_cursor:
        cui = str(cui)
        tui = str(tui)
        concept_map[cui][1].add(st_id_name[tui])
    mrsty_cursor.close()

    pickle.dump(concept_map, open("concepts.pickle", "wb"))
    pickle.dump(st_id_name, open("st_id_name.pickle", "wb"))


def compute_interested_concepts():
    '''
    put all our interested_st into a table along with their id
    put all our interested_rl into a table
    create another table with all the interested concepts. we can get this by doing a join of out interested_st with mrsty

    preprocess and store these in mem for faster processing:
    store all the interested concepts and their names in memory
    store mapping for interested concepts to their list of semantic types too

  '''

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)

    interested_st_id = []
    st_id_query = 'select ui from SRDEF where sty_rl="%s"'
    for st in interested_st:
        st_id_cursor = cnx.cursor()
        st_id_cursor.execute(st_id_query % st)
        if st_id_cursor.rowcount != 1:
            print 'No rows for %s' % st
        else:
            row = st_id_cursor.fetchone()
            st_id = str(row[0])
            interested_st_id.append(st_id)
        st_id_cursor.close()

    global interested_concepts
    st_concept_count = []
    st_id_to_concept_query = 'select cui from MRSTY where tui="%s"'
    cui_to_name_query = 'select str from MRCONSO where cui="%s"'

    for i in range(len(interested_st_id)):
        st_id = interested_st_id[i]
        st_name = interested_st[i]
        cui_cursor = cnx.cursor()
        cui_cursor.execute(st_id_to_concept_query % st_id)
        st_concept_count.append(cui_cursor.rowcount)
        for row in cui_cursor:
            cui = row[0]
            if cui not in interested_concepts:
                cui_name_cursor = cnx.cursor()
                cui_name_cursor.execute(cui_to_name_query % cui)
                if cui_name_cursor.rowcount == 0:
                    print 'No rows for %s' % cui
                else:
                    row = cui_name_cursor.fetchone()
                    interested_concepts[cui] = (row[0].lower(), set())
                cui_name_cursor.close()
            interested_concepts[cui][1].add(st_name)
        cui_cursor.close()
    print 'interested_concepts size={}'.format(len(interested_concepts))
    for i in range(len(interested_st)):
        print interested_st[i], st_concept_count[i]

    # clean names to make it ascii compatible, and categorize concepts into our broad 3 categories
    for cui, val in interested_concepts.iteritems():
        name, st_set = val
        ascii_name = name.encode('ascii', 'ignore')
        ascii_st_set = set()
        c_type = [False, False, False]
        for st in st_set:
            ascii_st_set.add(st.encode('ascii', 'ignore'))
            # the broad concept type vector (treatment, disease, theme)
            if st in treatments:
                c_type[0] = True
            elif st in diseases:
                c_type[1] = True
            else:
                c_type[2] = True

        interested_concepts[cui] = (ascii_name, ascii_st_set, c_type)

    pickle.dump(interested_concepts, open("interested_concepts.pickle", "wb"))

    cnx.close()


def extract_interested_relations():
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)
    rel_cursor = cnx.cursor()
    rel_cursor.execute('select cui1, rela, cui2 from cleaned_mrrel')
    c = 0
    c1 = 0
    c2 = 0
    f1 = open('mrrel1.txt', 'w')
    f2 = open('mrrel2.txt', 'w')
    f2_cui = open('mrrel2_cui.txt', 'w')
    f3 = open('mrrel3.txt', 'w')
    f3_cui = open('mrrel3_cui.txt', 'w')

    insert_mrrel2_query = 'insert into mrrel2 values ("{}","{}","{}");'
    insert_mrrel3_query = 'insert into mrrel3 values ("{}","{}","{}");'

    insert_cursor = cnx.cursor()

    for (cui1, rela, cui2) in rel_cursor:
        c1in = cui1 in interested_concepts
        c2in = cui2 in interested_concepts
        if c1in or c2in:
            f1.write('\t'.join((cui2, cui1, rela)) + '\n')
            c += 1
            if c1in and c2in:
                c1_name = interested_concepts[cui1][0]
                c2_name = interested_concepts[cui2][0]
                f2.write('\t'.join((c2_name, c1_name, rela)) + '\n')
                f2_cui.write('\t'.join((cui2, cui1, rela)) + '\n')
                insert_cursor.execute(insert_mrrel2_query.format(cui2, cui1,
                                                                 rela))
                c1 += 1
                if rela in interested_rl:
                    f3.write('\t'.join((c2_name, c1_name, rela)) + '\n')
                    f2_cui.write('\t'.join((cui2, cui1, rela)) + '\n')
                    insert_cursor.execute(insert_mrrel3_query.format(
                        cui2, cui1, rela))
                    c2 += 1

    rel_cursor.close()
    print c, c1, c2
    f1.close()
    f2.close()
    f3.close()
    f2_cui.close()
    f3_cui.close()
    cnx.commit()
    cnx.close()

def augment_type_into_concepts():
    global concept_map
    if len(concept_map) == 0:
        concept_map = pickle.load(open("concepts.pickle", "rb"))

    for cui, val in concept_map.iteritems():
        name, st_set = val
        name = name.lower()
        c_type = [False, False, False]
        for st in st_set:
            # the broad concept type vector (treatment, disease, theme)
            if st in treatments:
                c_type[0] = True
            elif st in diseases:
                c_type[1] = True
            else:
                c_type[2] = True

        concept_map[cui] = (name, st_set, c_type)

    pickle.dump(concept_map, open("concepts_with_type.pickle", "wb"))

def derive_reverse_concepts_map():
    global concept_map
    if len(concept_map) == 0:
        concept_map = pickle.load(open("concepts_with_type.pickle", "rb"))

    c = 0
    concept_name_map = {}

    for cui, (name, st_set, c_type) in concept_map.iteritems():
        name = name.lower()
        if name in concept_name_map:
            c += 1 
            ctype = [t1 or t2 for t1, t2 in zip(c_type, concept_name_map[name][2])]
        concept_name_map[name] = (cui, st_set, c_type)

    print c

    pickle.dump(concept_name_map, open("concept_name_map.pickle", "wb"))

def extract_relaxed_relations():
    global concept_map
    if len(concept_map) == 0:
        concept_map = pickle.load(open("concepts.pickle", "rb"))

    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)
    rel_cursor = cnx.cursor()
    rel_cursor.execute('select cui1, rela, cui2 from cleaned_mrrel')
    c = 0
    f = open('mrrel1.txt', 'w')
    f_cui = open('mrrel1_cui.txt', 'w')

    for (cui1, rela, cui2) in rel_cursor:
        if (cui1 in interested_concepts) or (cui2 in interested_concepts):
            c1_name = concept_map[cui1][0]
            c2_name = concept_map[cui2][0]

            rela = rl_map.get(rela, rela)

            f_cui.write('\t'.join((cui2, cui1, rela)) + '\n')
            c += 1
            f.write('\t'.join((c2_name, c1_name, rela)) + '\n')

    rel_cursor.close()
    print c
    f.close()
    f_cui.close()
    cnx.commit()
    cnx.close()


def extract_relations(lvl):
    global concept_map
    if len(concept_map) == 0:
        concept_map = pickle.load(open("concepts.pickle", "rb"))

    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)
    rel_cursor = cnx.cursor()
    rel_cursor.execute('select cui1, rela, cui2 from cleaned_mrrel')
    c = 0
    f = open('mrrel' + str(lvl) + '.txt', 'w')
    f_cui = open('mrrel' + str(lvl) + '_cui.txt', 'w')

    for (cui1, rela, cui2) in rel_cursor:
        c1in = cui1 in interested_concepts
        c2in = cui2 in interested_concepts

        c1_name = concept_map[cui1][0]
        c2_name = concept_map[cui2][0]

        satisfied = False

        if lvl == 1:
            if c1in or c2in:
                satisfied = True
        elif lvl == 2:
            if c1in and c2in:
                satisfied = True
        elif lvl == 3:
            if c1in and c2in and rela in rl_map:
                satisfied = True

        if satisfied:
            c += 1
            rela = rl_map.get(rela, rela)
            f_cui.write('\t'.join((cui2, cui1, rela)) + '\n')
            f.write('\t'.join((c2_name, c1_name, rela)) + '\n')

    rel_cursor.close()
    print c
    f.close()
    f_cui.close()
    cnx.commit()
    cnx.close()


def output_sem_links_and_sem_nw():
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    f = open('sem_interested_isa_links.txt', 'w')
    rl = 'isa'
    for cui, (name, st_set, st_vec) in interested_concepts.iteritems():
        for st_name in st_set:
            f.write('\t'.join((name, st_name, rl)) + '\n')
    f.close()

    global concept_map
    if len(concept_map) == 0:
        concept_map = pickle.load(open("concepts.pickle", "rb"))

    f = open('sem_all_isa_links.txt', 'w')
    rl = 'isa'
    for cui, (name, st_set) in concept_map.iteritems():
        for st_name in st_set:
            f.write('\t'.join((name, st_name, rl)) + '\n')
    f.close()

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)
    sem_nw_cursor = cnx.cursor()
    sem_nw_cursor.execute('select sty1, rl, sty2 from SRSTRE2')

    f = open('sem_nw_expanded.txt', 'w')
    for (sty1, rl, sty2) in sem_nw_cursor:
        f.write('\t'.join((str(sty1), str(sty2), str(rl))) + '\n')
    f.close()

    sem_nw_cursor.close()

    sem_nw_cursor = cnx.cursor()
    sem_nw_cursor.execute('select sty_rl1, rl, sty_rl2 from SRSTR')

    f = open('sem_nw.txt', 'w')
    for (sty1, rl, sty2) in sem_nw_cursor:
        f.write('\t'.join((str(sty1), str(sty2), str(rl))) + '\n')
    f.close()

    sem_nw_cursor.close()

    cnx.close()

def extract_diseases_concepts():
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    diseases = ['Pneumonia']
    diseases = [s.lower() for s in diseases]
    disease_concepts = set()
    for cui, (name, st_set, st_vec) in interested_concepts.iteritems():
        if not st_vec[1]:
            continue
        name = name.lower()
        for disease in diseases:
            if disease in name:
                disease_concepts.add(name)

    return disease_concepts

def extract_diseases_concept_cui(disease):
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    disease = disease.lower()
    disease_len = len(disease)
    disease_concepts_cui = set()
    for cui, (name, st_set, st_vec) in interested_concepts.iteritems():
        if not st_vec[1]:
            continue
        name = name.lower()
        if (disease in name) and (len(name.split()) <= disease_len + 2):
                disease_concepts_cui.add(cui)

    return disease_concepts_cui

def generate_disease_map():
    reduced_diseases = ['Influenza', 'AIDS', 'COPD', 'Tuberculosis', 'Atrial Fibrillation', 'Psoriasis', 'Osteoarthritis', 'Diabetes', 'Arthritis', 'Pneumonia']

    disease_map = {}
    for disease in reduced_diseases:
        cuis = extract_diseases_concept_cui(disease)
        disease_map[disease] = cuis

    for disease, cuis in disease_map.iteritems():
        print disease, len(cuis)


    pickle.dump(disease_map, open("disease_map.pickle", "wb"))

    return disease_map
    
def generate_reduced_disease_treatment_pairs(to_file = True):
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    reduced_diseases = ['Influenza', 'AIDS', 'COPD', 'Tuberculosis', 'Atrial Fibrillation', 'Psoriasis', 'Osteoarthritis', 'Diabetes', 'Arthritis', 'Pneumonia']

    reduced_diseases = ['D' + name.lower() for name in reduced_diseases]

    treatments = set()

    if to_file:
        f_out = open('reduced_treatment_pairs.tsv', 'w')
    else:
        # pairs hold treatment-disease pairs
        pairs = []

    for cui, (name, st_set, st_vec) in interested_concepts.iteritems():
        if st_vec[0]:
            # its a treatment
            treatments.add(cui)

    for treatment in treatments:
        current_pairs = [(treatment, disease) for disease in reduced_diseases]
        if not to_file:
            pairs.extend(current_pairs)
        else:
            for pair in current_pairs:
                f_out.write('\t'.join(pair) + '\n')
    if to_file:
        f_out.close()
    else:
        return pairs

def generate_disease_treatment_pairs(to_file = True):
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    disease_concepts = extract_diseases_concepts()

    treatments = set()

    if to_file:
        f_out = open('pairs.tsv', 'w')
    else:
        # pairs hold treatment-disease pairs
        pairs = []

    for cui, (name, st_set, st_vec) in interested_concepts.iteritems():
        if st_vec[0]:
            # its a treatment
            treatments.add(name)

    for treatment in treatments:
        current_pairs = [(treatment, disease) for disease in disease_concepts]
        if not to_file:
            pairs.extend(current_pairs)
        else:
            for pair in current_pairs:
                f_out.write('\t'.join(pair) + '\n')
    if to_file:
        f_out.close()
    else:
        return pairs


def get_relation_stats():
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    # relation count matrix based on entity pair type
    # types :- 1 - treatment-theme, 2 - theme-disease, 3 - treatment-disease
    rel_count = {i: {rel: 0 for rel in interested_rl} for i in [1, 2, 3]}

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)
    mrrel3_cursor = cnx.cursor()
    mrrel3_cursor.execute('select c1, c2, rel from mrrel3')

    total = 0
    extra_counted = 0
    for (c1, c2, rel) in mrrel3_cursor:
        total += 1
        c1_name, c1_st, c1_type = interested_concepts[c1]
        c2_name, c2_st, c2_type = interested_concepts[c2]
        counted = 0
        # treatment-theme pair
        if (c1_type[0] and c2_type[2]) or (c2_type[0] and c1_type[2]):
            rel_count[1][rel] += 1
            counted += 1
        if (c1_type[1] and c2_type[2]) or (c2_type[1] and c1_type[2]):
            rel_count[2][rel] += 1
            counted += 1
        if (c1_type[0] and c2_type[1]) or (c2_type[0] and c1_type[1]):
            rel_count[3][rel] += 1
            counted += 1
        extra_counted += max(0, counted - 1)
    print total, extra_counted
    print rel_count

    for e_type, rel_dist in rel_count.iteritems():
        print e_type, sum(rel_dist.values())

    for rel in interested_rl:
        s = 0
        for i in [1, 2, 3]:
            s += rel_count[i][rel]
        print rel, s

    mrrel3_cursor.close()
    cnx.close()


def split_table_by_relations(table_name):
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)
    mrrel3_cursor = cnx.cursor()
    mrrel3_cursor.execute('select c1, c2, rel from ' + table_name)

    f_kb = open(table_name + 'kb.txt', 'w')
    f_may_treat = open(table_name + 'may_treat.txt', 'w')
    f_may_prevent = open(table_name + 'may_prevent.txt', 'w')

    for (c1, c2, rel) in mrrel3_cursor:

        c1_name, c1_st, c1_type = interested_concepts[c1]
        c2_name, c2_st, c2_type = interested_concepts[c2]

        if rel == 'may_treat':
            f_may_treat.write('\t'.join((c1_name, c2_name)) + '\n')
        elif rel == 'may_prevent':
            f_may_prevent.write('\t'.join((c1_name, c2_name)) + '\n')
        else:
            f_kb.write('\t'.join((c1_name, c2_name, rel)) + '\n')

    f_kb.close()
    f_may_treat.close()
    f_may_prevent.close()

    mrrel3_cursor.close()
    cnx.close()


def split_file_by_relations(fn):
    with open(fn, 'r') as f:
        f_kb = open('kb.txt', 'w')
        f_beneficial = open('beneficial.txt', 'w')
        f_harmful = open('harmful.txt', 'w')

        for line in f:
            c1, c2, rl = line.split('\t')
            rl = rl.strip()
            rl = rl_map.get(rl, rl)
            if rl == 'beneficial':
                f_beneficial.write('\t'.join((c1, c2)) + '\n')
            elif rl == 'harmful':
                f_harmful.write('\t'.join((c1, c2)) + '\n')
            else:
                f_kb.write('\t'.join((c1, c2, rl)) + '\n')

        f_kb.close()
        f_beneficial.close()
        f_harmful.close()


def split_file_by_relations_specific(fn):
    global concept_name_map
    if len(concept_name_map) == 0:
        concept_name_map = pickle.load(open("concept_name_map.pickle", "rb"))

    c = 0
    with open(fn, 'r') as f:
        f_kb = open('kb.txt', 'w')
        f_beneficial = open('beneficial.txt', 'w')
        f_harmful = open('harmful.txt', 'w')

        for line in f:
            c1, c2, rl = line.split('\t')
            c1, c2, rl = c1.lower().strip(), c2.lower().strip(), rl.strip()
            rl = rl_map.get(rl, rl)
            try:
                # type vec: (treatment, disease, theme)
                treatment_disease = concept_name_map[c1][2][0] and concept_name_map[c2][2][1]
                if rl == 'beneficial' and treatment_disease:
                    f_beneficial.write('\t'.join((c1, c2)) + '\n')
                elif rl == 'harmful' and treatment_disease:
                    f_harmful.write('\t'.join((c1, c2)) + '\n')
                else:
                    f_kb.write('\t'.join((c1, c2, rl)) + '\n')
            except:
                c += 1
            

        f_kb.close()
        f_beneficial.close()
        f_harmful.close()
    print c

def clean(fn, nt = 2, print_lines = False):
    c = 0
    c1 = 0
    c2 = 0
    c3 = 0
    total = 0

    with open(fn, 'r') as f, open('cleaned.txt' , 'w') as f_cleaned:
        for line in f:
            total += 1
            clean = False
            fields = line.split('\t')
            fields = [field.strip() for field in fields]
            if len(fields) != nt + 1:
                c += 1
                c1 += 1
                clean = True
            elif line.count('\t') != nt:
                c += 1
                c2 += 1
                clean = True
            elif any([field == '' for field in fields]):
                c += 1
                c3 += 1
                clean = True
            if not clean:
                f_cleaned.write(line)
            elif print_lines:
                print line

    print 'Total lines: {}'.format(total)
    print 'Total lines cleaned: {}'.format(c)
    print 'Insufficient elems lines cleaned: {}'.format(c1)
    print 'Insufficient tabs lines cleaned: {}'.format(c2)
    print 'Empty entity lines cleaned: {}'.format(c3)


def filter_file(fn, filter_set, print_lines = False):
    c = 0
    total = 0

    with open(fn, 'r') as f, open('cleaned.txt' , 'w') as f_cleaned:
        for line in f:
            total += 1
            remove = False
            
            for field in line.split('\t'):
                field = field.strip()
                if field in filter_set:
                    remove = True
                    c += 1
                    break

            if not remove:
                f_cleaned.write(line)
            elif print_lines:
                print line

    print total, c



def parse_results(fn, testing_only = False):
    c_all = 0
    c_no_pred = 0
    c_training = 0
    c_testing = 0
    c_new = 0
    results = []
    with open(fn, 'r') as f:
        for line in f:
            fields = line.split('\t')
            for i in range(len(fields)):
                fields[i] = fields[i].strip()
            while '' in fields:
                fields.remove('')
            l = len(fields)
            c_all += 1
            if l < 3:
                c_no_pred += 1
                continue
            elif l == 3:
                # if fields[2][0] == '*':
                #     continue
                if not testing_only:
                    results.append((float(fields[2]), fields[0], fields[1]))
                c_new += 1
            elif l == 4:
                # is training or testing
                if fields[3] == '*' or fields[3] == '^':
                    # testing
                    if testing_only:
                        results.append((float(fields[2]), fields[0], fields[1]))
                    c_testing += 1
                elif fields[3] == '*^':
                    # training
                    c_training += 1
                else:
                    print 'somehting wrong. length 4, but not train/test'
                    print line
                    print 'treatment:' + fields[0]
                    print 'disease:' + fields[1]
            else:
                    print 'somehting wrong. length > 4'
                    print line
                    print 'treatment:' + fields[0]
                    print 'disease:' + fields[1]

    print c_all, c_new, c_training, c_testing, c_no_pred
    results.sort(reverse=True)
    for result in results:
        print result

def tmp(fn):
    c = 0
    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))
    interested_name_map = {}
    for cui, (name, st_set, c_type) in interested_concepts.iteritems():
        name = name.lower()
        if name in interested_name_map:
            c += 1 
            ctype = [t1 or t2 for t1, t2 in zip(c_type, interested_name_map[name][2])]
        interested_name_map[name] = (cui, st_set, c_type)

    cnt1 = 0
    with open(fn, 'r') as f:
        f_kb = open('kb.txt', 'w')
        f_beneficial = open('beneficial.txt', 'w')
        f_harmful = open('harmful.txt', 'w')

        for line in f:
            c1, c2, rl = line.split('\t')
            c1, c2, rl = c1.lower().strip(), c2.lower().strip(), rl.strip()
            rl = rl_map.get(rl, rl)
            try:
                # type vec: (treatment, disease, theme)
                treatment_disease = concept_name_map[c1][2][0] and concept_name_map[c2][2][1]
                if rl == 'beneficial' and treatment_disease:
                    f_beneficial.write('\t'.join((c1, c2)) + '\n')
                elif rl == 'harmful' and treatment_disease:
                    f_harmful.write('\t'.join((c1, c2)) + '\n')
                else:
                    f_kb.write('\t'.join((c1, c2, rl)) + '\n')
            except:
                cnt1 += 1
            

        f_kb.close()
        f_beneficial.close()
        f_harmful.close()


    print c, cnt1

def tmp2(fn):

    global interested_concepts
    if len(interested_concepts) == 0:
        interested_concepts = pickle.load(open("interested_concepts.pickle",
                                               "rb"))
    num_treat_c1 = 0
    num_treat_c2 = 0
    num_disease_c1 = 0
    num_disease_c2 = 0
    with open(fn, 'r') as f:
        f_kb = open('kb.txt', 'w')
        f_beneficial = open('beneficial.txt', 'w')
        f_harmful = open('harmful.txt', 'w')

        for line in f:
            c2, rl, c1 = line.split(':')
            c1, c2, rl = c1.strip(), c2.strip(), rl.strip()
            rl = rl_map[rl]

            # type vec: (treatment, disease, theme)
            if interested_concepts[c1][2][0]:
                num_treat_c1 += 1

            if interested_concepts[c1][2][1]:
                num_disease_c1 += 1

            if interested_concepts[c2][2][0]:
                num_treat_c2 += 1

            if interested_concepts[c2][2][1]:
                num_disease_c2 += 1

            treatment_disease = interested_concepts[c1][2][0] and interested_concepts[c2][2][1]
            #treatment_disease = any(interested_concepts[c1][2]) or any(interested_concepts[c2][2])
            
            c1 = interested_concepts[c1][0]
            c2 = interested_concepts[c2][0]

            if rl == 'beneficial' and treatment_disease:
                f_beneficial.write('\t'.join((c1, c2)) + '\n')
            elif rl == 'harmful' and treatment_disease:
                f_harmful.write('\t'.join((c1, c2)) + '\n')
            else:
                f_kb.write('\t'.join((c1, c2, rl)) + '\n')

        f_kb.close()
        f_beneficial.close()
        f_harmful.close()


    print num_treat_c1
    print num_treat_c2
    print num_disease_c1
    print num_disease_c1



def check_names():
    global concept_map
    if len(concept_map) == 0:
        concept_map = pickle.load(open("concepts.pickle", "rb"))

    concept_name_map = {}

    for cui, val in concept_map.iteritems():
        name = val[0]
        concept_name_map[name] = cui

    nf = 0
    f = 0
    t = 0

    all_e = set()
    has_rev_map = set()
    no_rev_map = set()

    for line in open('surface-relations.tsv','r'):
        t += 1
        fields = line.split('\t')
        fields = [field.strip() for field in fields]
        e1, e2 = fields[0], fields[2]
        all_e.add(e1)
        if (e1 not in concept_name_map):
            nf += 1
            no_rev_map.add(e1)
        else:
            f += 1
            has_rev_map.add(e1)
        all_e.add(e2)
        if (e2 not in concept_name_map):
            nf += 1
            no_rev_map.add(e2)
        else:
            f += 1
            has_rev_map.add(e2)
    
    print t, f, nf
    print len(all_e), len(has_rev_map), len(no_rev_map)
    print no_rev_map


def find_cui_from_db(fn, kb=False):

    all_e = set()
    has_rev_map = set()
    no_rev_map = set()

    name_cui = {}

    for line in open(fn,'r'):
        fields = line.split('\t')
        fields = [field.strip() for field in fields]
        all_e.add(fields[0])
        if kb:
            all_e.add(fields[1])
        else:
            all_e.add(fields[2])

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)
    q_cursor = cnx.cursor()

    c = 0

    for e in all_e:
        q_cursor.execute('select distinct cui from MRCONSO where str = "%s" COLLATE utf8_general_ci' % e) 

        if q_cursor.rowcount < 1:
            no_rev_map.add(e)
        else:
            if q_cursor.rowcount > 1:
                c += 1
            cui = str(q_cursor.fetchone()[0])
            name_cui[e] = cui
            has_rev_map.add(e)

    q_cursor.close()
    cnx.close()

    print len(all_e), len(has_rev_map), len(no_rev_map)
    print c
    print no_rev_map

    return name_cui, no_rev_map

def dump_mrrel():

    cnx = mysql.connector.connect(user='root',
                                  password='mysqlroot',
                                  database='umlsdb',
                                  buffered=True)
    rel_cursor = cnx.cursor()
    rel_cursor.execute('select cui1, rela, cui2 from cleaned_mrrel')
    c = 0
    f_out = open('mrrel.txt', 'w')
    
    for (cui1, rela, cui2) in rel_cursor:
        f_out.write('\t'.join((cui2, cui1, rela)) + '\n')

    rel_cursor.close()
    f_out.close()

    cnx.close()


def TwoHopSubgraph(G, root):

    if root not in G:
        return set()

    limit = 2
    # even if loops are there, we traveserse them
    # shouldnt harm since its depth limited
    currentLevel = [root]
    breadth = 0
    subG = set()
    while currentLevel and breadth < limit:
        nextLevel = set()
        levelGraph = {v:set() for v in currentLevel}
        for v in currentLevel:
            for w in G[v]:
                levelGraph[v].add(w)
                nextLevel.add(w)
                for rel in G[v][w]:
                    subG.add((v, w, rel))
        breadth += 1
        currentLevel = nextLevel

    return subG

def change_to_cui_or_disease(fn, t=1):
    # t=1: surface relations
    # t=2: kb rel
    # t=3: ground truth. change diseases
    
    other_entities = pickle.load(open("other_entities.pickle", "rb"))

    disease_map = pickle.load(open("disease_map.pickle", "rb"))
    disease_reverse_map = {}

    for name, cui_set in disease_map.iteritems():
        name = 'D' + name.lower()
        for cui in cui_set:
            disease_reverse_map[cui] = name

    with open(fn, 'r') as f, open('changed.txt' , 'w') as f_changed:
        for line in f:
            fields = line.split('\t')
            fields = [field.strip() for field in fields]
            if t == 2 or t == 3:
                e1, e2, rel = fields
            else:
                e1, rel, e2 = fields

            cui1 = other_entities[e1]

            if t==3:
                cui2 = 'D' + e2.lower()
            else:
                cui2 = other_entities[e2]

            if cui1 in disease_reverse_map:
                cui1 = disease_reverse_map[cui1]

            if cui2 in disease_reverse_map:
                cui2 = disease_reverse_map[cui2]

            if t == 2 or t == 3:
                f_changed.write('\t'.join((cui1, cui2, rel)) + '\n')
            else:
                f_changed.write('\t'.join((cui1, rel, cui2)) + '\n')


def collapse(edges, mapping, is_kb = True, file_name = None):

    print 'Number of edges before collapsing:', len(edges)

    to_remove = set()
    to_add = set()
    for edge in edges:
        if is_kb:
            (cui1, cui2, rel) = edge
        else:
            (cui1, rel, cui2) = edge
        old_edge = edge
        modified = False
        if cui1 in mapping:
            cui1 = mapping[cui1]
            modified = True
        if cui2 in mapping:
            cui2 = mapping[cui2]
            modified = True
        if modified:
            edge = (cui1, cui2, rel) if is_kb else (cui1, rel, cui2)
            to_remove.add(old_edge)
            to_add.add(edge)


    edges = edges.difference(to_remove)
    print 'Number of edges:', len(edges)
    edges = edges.union(to_add)

    print 'Number of edges to remove:', len(to_remove)
    print 'Number of edges to add:', len(to_add)
    print 'Number of edges after collapsing:', len(edges)

    if file_name:
        f_out = open(file_name, 'w')
        for edge in edges:
            f_out.write('\t'.join(edge) + '\n')
        f_out.close()

    return edges

def build_graph(file_name, is_di = True, is_kb = True):
    if is_di:
        g = nx.MultiDiGraph()
    else:
        g = nx.MultiGraph()
    with open(file_name, 'r') as f:
        for line in f:
            fields = line.split('\t')
            fields = [field.strip() for field in fields]
            if is_kb:
                e1, e2, rel = fields
            else:
                e1, rel, e2 = fields
            g.add_edge(e1, e2, key=rel)

    return g

def extract_restricted_graph():

    if os.path.isfile('mrrel.pickle'):
        g = pickle.load(open("mrrel.pickle", "rb"))
    else:
        g = build_graph('mrrel.txt')
        pickle.dump(g, open("mrrel.pickle", "wb"))

    other_entities = pickle.load(open("other_entities.pickle", "rb"))

    restricted_graph = set()
    for e, cui in other_entities.iteritems():
        e_subgraph = TwoHopSubgraph(g, cui)
        restricted_graph.update(e_subgraph)
    
    print 'Done with 2hops for other entities'

    disease_map = pickle.load(open("disease_map.pickle", "rb"))
    disease_reverse_map = {}

    all_disease_cui = set()

    for name, cui_set in disease_map.iteritems():
        all_disease_cui.update(cui_set)
        name = 'D' + name.lower()
        for cui in cui_set:
            disease_reverse_map[cui] = name

    for cui in all_disease_cui:
        e_subgraph = TwoHopSubgraph(g, cui)
        restricted_graph.update(e_subgraph)

    print 'Done with 2hops for disease cuis'

    f_out = open('restricted_graph.tsv', 'w')

    restricted_graph = collapse(restricted_graph, disease_reverse_map, True, 'restricted_graph.tsv')

    f_out.close()

    print 'Done collapsing diseases'

    print len(to_remove)

    print len(restricted_graph)
    pickle.dump(restricted_graph, open("restricted_graph.pickle", "wb"))

    print 'Done'

def filter_infrequent_relations(fn, limit, filter_digits = True, is_kb = False):


    with open(fn, 'r') as f, open('filtered.txt' , 'w') as f_filtered:
        rel_instances = {}
        rel_count = {}
        for line in f:
            fields = line.split('\t')
            fields = [field.strip() for field in fields]
            if is_kb:
                e1, e2, rel = fields
            else:
                e1, rel, e2 = fields

            if filter_digits and (any(d in e1 for d in'0123456789') or any(d in e2 for d in'0123456789')):
                continue

            if rel not in rel_instances:
                rel_instances[rel] = set()
                rel_count[rel] = 0

            rel_instances[rel].add((e1, e2))
            rel_count[rel] += 1

        for rel, count in rel_count.iteritems():
            #print rel, count
            if count <= limit:
                rel_instances.pop(rel)
            else:
                for (e1, e2) in rel_instances[rel]:
                    if is_kb:
                        f_filtered.write('\t'.join((e1, e2, rel)) + '\n')
                    else:
                        f_filtered.write('\t'.join((e1, rel, e2)) + '\n')

    print len(rel_count)
    print len(rel_instances)


def collapse_mrrel2():
    disease_map = pickle.load(open("disease_map.pickle", "rb"))
    disease_reverse_map = {}

    all_disease_cui = set()

    for name, cui_set in disease_map.iteritems():
        all_disease_cui.update(cui_set)
        name = 'D' + name.lower()
        for cui in cui_set:
            disease_reverse_map[cui] = name

    mrrel2 = set()
    for line in open('mrrel2_cui.txt', 'r'):
        fields = line.split('\t')
        edge = tuple(field.strip() for field in fields)
        mrrel2.add(edge)

    print 'Number of edges in mrrel2:', len(mrrel2)

    collapse(mrrel2, disease_reverse_map, True, 'mrrel2_cui_collapsed.tsv')

def add_column(file_name, col):
    with open(file_name, 'r') as f, open('added.tsv' , 'w') as f_added:
        for line in f:
            fields = line.split('\t')
            fields = [field.strip() for field in fields]
            fields.append(col)
            f_added.write('\t'.join(fields) + '\n')

def swap_col(file_name, i, j):
    '''
        Swap columns i and j in the given file
    '''
    i -= 1
    j -= 1
    with open(file_name, 'r') as f, open('convereted.tsv' , 'w') as f_converted:
        for line in f:
            fields = line.split('\t')
            fields = [field.strip() for field in fields]
            fields[i], fields[j] = fields[j], fields[i]
            f_converted.write('\t'.join(fields) + '\n')


def load_inverse_map():
    global inverse
    if not len(inverse):
        with open('inverse_map.tsv', 'r') as f:
            for line in f:
                fields = line.split()
                fields = [field.strip() for field in fields]
                if len(fields) == 1:
                    inverse[fields[0]] = fields[0]
                elif len(fields) == 2:
                    inverse[fields[1]] = fields[0]
                    inverse[fields[0]] = fields[1]
                else:
                    print 'Extra inverses in line:'
                    print line

    return inverse

def remove_inverse(file_name):

    inverse = load_inverse_map()

    edges = set()
    inverse_edges = set()

    with open(file_name, 'r') as f:
        for line in f:
            fields = line.split('\t')
            edge = tuple(field.strip() for field in fields)
            if (edge not in edges) and (edge not in inverse_edges):
                (cui1, cui2, rel) = edge
                edges.add(edge)
                inverse_edges.add((cui2, cui1, inverse[rel]))
            
            
    with open('cleaned.tsv', 'w') as f_cleaned:
        for edge in edges:
            f_cleaned.write('\t'.join(edge) + '\n')


def remove_beneficial_relations(file_name, inverse_only = False):
    inverse = load_inverse_map()
    rl_to_remove = set()
    for rl in interested_rl:
        if not inverse_only:
            rl_to_remove.add(rl)
        rl_to_remove.add(inverse[rl])

    print rl_to_remove

    with open(file_name, 'r') as f, open('cleaned.tsv', 'w') as f_cleaned:
        for line in f:
            fields = line.split('\t')
            edge = tuple(field.strip() for field in fields)
            (cui1, cui2, rel) = edge
            
            if rel not in rl_to_remove:
                f_cleaned.write('\t'.join(edge) + '\n')


def convert_cui_name(file_name, ftype = 1, use_interested_concepts = False):
    '''
        converts non collpased cuis into their names
        ftype:
        1 - kb
        2 - surface relations
        3 - training / testing data
    '''

    if use_interested_concepts:
        global interested_concepts
        if len(interested_concepts) == 0:
            interested_concepts = pickle.load(open("interested_concepts.pickle",
                                                   "rb"))
        mapping = interested_concepts
    else:
        global concept_map
        if len(concept_map) == 0:
            concept_map = pickle.load(open("concepts.pickle", "rb"))
        mapping = concept_map

    with open(file_name, 'r') as f, open('convereted.tsv' , 'w') as f_converted:
        for line in f:
            fields = line.split('\t')
            fields = [field.strip() for field in fields]
            if ftype == 1:
                e1, e2, rel = fields
            elif ftype == 2:
                e1, rel, e2 = fields
            elif ftype == 3:
                if len(fields) == 2:
                    e1, e2 = fields
                else:
                    e1, e2, sign = fields
            if e1[0] == 'C':
                # its a cui and not a collapsed disease
                e1 = mapping[e1][0]
            if e2[0] == 'C':
                # its a cui and not a collapsed disease
                e2 = mapping[e2][0]

            if ftype == 1:
                fields = [e1, e2, rel]
            elif ftype == 2:
                fields = [e1, rel, e2]
            elif ftype == 3:
                if len(fields) == 2:
                    fields = [e1, e2]
                else:
                    fields = [e1, e2, sign]

            f_converted.write('\t'.join(fields) + '\n')


def get_degrees(fn):
    concept_map = pickle.load(open("concepts.pickle", "rb"))

    g = build_graph(fn)
    d = g.out_degree()
    all_deg = sorted(d.items(), key=lambda (k, v): v, reverse=True)
    d = filter(lambda (k, v): k[0] == 'C', all_deg)
    for i in range(len(d)):
        cui, deg = d[i]
        name = concept_map[cui][0]
        d[i] = (name, cui, deg)

    with open('deg_mrrel_di.tsv', 'w') as f_out:
        for tup in d:
            f_out.write('\t'.join([str(e) for e in tup]) + '\n')

    return all_deg

#compute_concepts_map()
#augment_type_into_concepts()
#derive_reverse_concepts_map()
#compute_interested_concepts()
#extract_interested_relations()
#extract_relaxed_relations()
#extract_relations(3)
#output_sem_links_and_sem_nw()
#split_table_by_relations('mrrel3')
#split_file_by_relations('mrrel2.txt')
#split_file_by_relations_specific('mrrel3.txt')
#clean('mrrel2_collapsed_kb.txt', 2, True)
#tmp('/home/vgottipati/Project/mrrel3.txt')
#tmp2('/home/vgottipati/Project/Data/mrrel3_cui_inverse.txt')
#parse_results('/home/vgottipati/Project/pra_mem_tmp/examples/results/mrrel2_90/beneficial/scores.tsv')
#generate_disease_treatment_pairs()
#print extract_diseases_concepts()
#dump_mrrel()
#find_cui_from_db('surface-relations.tsv')

#change_to_cui_or_disease('surface-relations.tsv', 1)
#split_file_by_relations('restricted_graph.tsv')
#generate_reduced_disease_treatment_pairs()


# filter_infrequent_relations('surface-relations.tsv', 5)

# a, missing_e = find_cui_from_db('filtered.txt')
# b, _ = find_cui_from_db('ground-truth.tsv', True)
# #filter_file('surface-relations.tsv', missing_e)

# a.update(b)
# pickle.dump(a, open("other_entities.pickle", "wb"))

# extract_restricted_graph()



#add_column('mrrel2_cui_collapsed_harmful.tsv', '-1')
#swap_col('surface_relations_cui.tsv', 2, 3)
#remove_inverse('mrrel2_kb.txt')
#remove_beneficial_relations('mrrel2_ni_cui_collapsed_kb.tsv')
#convert_cui_name('pra_mem_tmp/examples/splits/mrrel2_cui/beneficial/training.tsv', 3, True)

# if len(interested_concepts) == 0:
#     interested_concepts = pickle.load(open("interested_concepts.pickle",
#                                                "rb"))

# if len(concept_map) == 0:
#     concept_map = pickle.load(open("concepts.pickle", "rb"))

# x = pickle.load(open("other_entities.pickle", "rb"))

# for line in open('ground-truth.tsv', 'r'):
#     fields = line.split('\t')
#     fields = [field.strip() for field in fields]
#     if fields[0] not in x:
#         print field
#     else:
#         cui1 = x[fields[0]]
#     if fields[1] not in x:
#         print field
#     else:
#         cui2 = x[fields[1]]
#     try:
#         concept_map[cui1]
#     except:
#         print fields[0], x[fields[0]]
#     try:
#         concept_map[cui2]
#     except:
#         print fields[1], x[fields[1]]


