#https://andialbrecht.wordpress.com/2009/03/29/sql-parsing-with-python-pt-ii/
import sqlparse
import sys
from more_itertools import unique_everseen

class Table:
    'Common base class for all tables'
    table_num = 0

    def __init__(self,name,col_list):
        self.name = name
        self.tuple_list = []
        self.col_list = col_list
        self.num = Table.table_num
        Table.table_num += 1

    def display_count(self):
        print(Table.table_num)

    def display_table(self,cols,schema,sumi,avg,maxi,mini,dist,count):
        if cols=='all':
            for i in self.col_list:
                print self.name+ '.' + i,
            print '\n'
            for mytuple in self.tuple_list:
                print ','.join([str(i) for i in mytuple])
            print '\n'
        else:
            x = 0
            xmax = []
            xmin = []
            xdist = []

            if sumi==1 or avg==1 or maxi==1 or mini==1 or dist==1 or count==1:
                for i in cols:
                    if i in self.col_list:
                        ind = self.col_list.index(i)
                        #print self.name + '.' + i
                        for mytuple in self.tuple_list:
                            if mytuple[0]!='':
                                if sumi==1:
                                    x = x + int(mytuple[ind])
                                elif avg==1:
                                    x = x+int(mytuple[ind])
                                elif maxi==1:
                                    xmax.append(int(mytuple[ind]))
                                elif mini==1:
                                    xmin.append(int(mytuple[ind]))
                                elif dist == 1:
                                    xdist.append(int(mytuple[ind]))
                        if sumi==1:
                            print 'sum(' + i + ')'
                            print x
                        elif avg==1:
                            print 'avg(' + i + ')'
                            print x/(len(self.tuple_list)*1.0)
                        elif maxi==1:
                            print 'max(' + i + ')'
                            print max(xmax)
                        elif mini==1:
                            print 'min(' + i + ')'
                            print min(xmin)
                        elif dist==1:
                            for i in list(unique_everseen(xdist)):
                                print i
                        elif count==1:
                            print 'count(' + i + ')'
                            print len(self.tuple_list)
                    elif i!='*':
                        print 'Column ' + i + ' not found.'
            else:
                col_inds = [self.col_list.index(k) for k in cols]
                for k in cols:
                    print self.name + '.' + k + ' ',
                print '\n'
                for k in self.tuple_list:
                    for j in col_inds:
                        print k[j],
                    print '\n'

    def read_data(self):
        filename = 'files/' + self.name + '.csv'
        fd = open(filename, 'r').read()
        f = fd.split('\r\n')
        sl = []
        g1=[]

        for i in f:
            if '"' not in i:
                g = i.split(',')
                g = tuple(g)
                if g[0]!='':
                    self.tuple_list.append(g)
            else:
                g = i.split('"')
                g1 = [k for k in g if k!='' and k!=',']
                if g1!=[]:
                    g = tuple(g1)
                if g[0]!='':
                    self.tuple_list.append(g)


class Query:
    def __init__(self,text):
        self.text = text
    def process_where(self,cols,dbs,where_clause,schema):
        print 'PROCESSING WHERE'
        col =[]
        tup = []
        for i in schema.table_list:
            if i.name == dbs[0]:
                print i.name
                col = i.col_list
                tup = i.tuple_list
        if cols!=['*']:
            col_inds = [col.index(i) for i in cols]
        else:
            col_inds = range(0,len(col))
        AND = 0
        OR = 0

        if 'and' in where_clause or 'AND' in where_clause:
            where1 = where_clause[0].strip('\n')
            where2 = where_clause[3].strip('\n')
            AND = 1
        elif 'or' in where_clause or 'OR' in where_clause:
            where1 = where_clause[0].strip('\n')
            where2 = where_clause[3].strip('\n')
            OR=1
        else:
            where1 = where_clause[0].strip('\n')
            where2 = ''
        cond2_value =''
        cond2_table = ''
        if '<=' in where1:
            cond1_table = where1.split('<=')[0]
            cond1_value = where1.split('<=')[1]
            op1=4
        elif '>=' in where1:
            cond1_table = where1.split('>=')[0]
            cond1_value = where1.split('>=')[1]
            op1=5
        elif '<' in where1:
            cond1_table = where1.split('<')[0]
            cond1_value = where1.split('<')[1]
            op1 = 1
        elif '>' in where1:
            cond1_table = where1.split('>')[0]
            cond1_value = where1.split('>')[1]
            op1=2
        elif '=' in where1:
            cond1_table = where1.split('=')[0]
            cond1_value = where1.split('=')[1]
            op1 =3
        if '<=' in where2:
            cond2_table = where2.split('<=')[0]
            cond2_value = where2.split('<=')[1]
            op2=4
        elif '>=' in where2:
            cond2_table = where2.split('>=')[0]
            cond2_value = where2.split('>=')[1]
            op2=5
        elif '<' in where2:
            cond2_table = where2.split('<')[0]
            cond2_value = where2.split('<')[1]
            op2 =1
        elif '>' in where2:
            cond2_table = where2.split('>')[0]
            cond2_value = where2.split('>')[1]
            op2=2
        elif '=' in where2:
            cond2_table = where2.split('=')[0]
            cond2_value = where2.split('=')[1]
            op2=3

        ind1 = col.index(cond1_table)
        if cond2_table!='' and cond2_value!='':
            ind2 = col.index(cond2_table)

        w1 = False
        w2 = False
        if AND==1:
            for k in tup:
                if op1==1 and int(k[ind1])<int(cond1_value):
                    w1 = True
                elif op1==2 and int(k[ind1])>int(cond1_value):
                    w1 = True
                elif op1==3 and int(k[ind1])==int(cond1_value):
                    w1 = True
                elif op1==4 and int(k[ind1])<=int(cond1_value):
                    w1 = True
                elif op1==5 and int(k[ind1])>=int(cond1_value):
                    w1 = True
                else:
                    w1 = False

                if op2==1 and int(k[ind2])<int(cond2_value):
                    w2 = True
                elif op2==2 and int(k[ind2])>int(cond2_value):
                    w2 = True
                elif op2==3 and int(k[ind2])==int(cond2_value):
                    w2 = True
                elif op2==4 and int(k[ind2])<=int(cond2_value):
                    w2 = True
                elif op2==5 and int(k[ind2])>=int(cond2_value):
                    w2 = True
                else:
                    w2 = False

                if (w1 and w2):
                    for s in col_inds:
                        print k[s],
                    print '\n'

        elif OR==1:
            for k in tup:
                if op1==1 and int(k[ind1])<int(cond1_value):
                    w1 = True
                elif op1==2 and int(k[ind1])>int(cond1_value):
                    w1 = True
                elif op1==3 and int(k[ind1])==int(cond1_value):
                    w1 = True
                elif op1==4 and int(k[ind1])<=int(cond1_value):
                    w1 = True
                elif op1==5 and int(k[ind1])>=int(cond1_value):
                    w1 = True
                else:
                    w1 = False

                if op2==1 and int(k[ind2])<int(cond2_value):
                    w2 = True
                elif op2==2 and int(k[ind2])>int(cond2_value):
                    w2 = True
                elif op2==3 and int(k[ind2])==int(cond2_value):
                    w2 = True
                elif op2==4 and int(k[ind2])<=int(cond2_value):
                    w2 = True
                elif op2==5 and int(k[ind2])>=int(cond2_value):
                    w2 = True
                else:
                    w2 = False

                if (w1 or w2):
                    for s in col_inds:
                        print k[s],
                    print '\n'


    def process_join(self,cols,dbs,schema):
        print 'processing join'
        flag = 0
        temp_col = []
        join_cols = []
        tup = []
        new_tup_list = []
        for i in dbs:
            for k in schema.table_list:
                if i == k.name and flag==0:
                    join_cols.append(k.col_list)
                    temp_col = temp_col+k.col_list
                    tup.append(k.tuple_list)

        for i in tup[1]:
            for j in tup[0]:
                g = j+i
                new_tup_list.append(tuple(g))
        temp_table = Table('temp',temp_col)
        temp_table.tuple_list = new_tup_list

        flag = 0
        for l in cols:
            if 'sum' in l:
                flag = 1
                if '(' in l:
                    new_cols = l.split('(')[1].split(')')[0]
                else:
                    new_cols = l
                temp_table.display_table(new_cols,schema,1,0,0,0,0,0)
            elif 'avg' in l:
                flag = 1
                if '(' in l:
                    new_cols = l.split('(')[1].split(')')[0]
                else:
                    new_cols = l
                temp_table.display_table(new_cols,schema,0,1,0,0,0,0)
            elif 'max' in l:
                flag = 1
                if '(' in l:
                    new_cols = l.split('(')[1].split(')')[0]
                else:
                    new_cols = l
                temp_table.display_table(new_cols,schema,0,0,1,0,0,0)
            elif 'min' in l:
                flag = 1
                if '(' in l:
                    new_cols = l.split('(')[1].split(')')[0]
                else:
                    new_cols = l
                temp_table.display_table(new_cols,schema,0,0,0,1,0,0)
            elif 'distinct' in l:
                flag = 1
                if '(' in l:
                    new_cols = l.split('(')[1].split(')')[0]
                else:
                    new_cols = l
                temp_table.display_table(new_cols,schema,0,0,0,0,1,0)
            elif '*' in l:
                flag = 1
                temp_table.display_table('all',schema,0,0,0,0,0,0)
        printing = []
        col_inds = []
        if flag==0:
            if cols!=['*']:
                col_inds = [temp_col.index(i) for i in cols]
            else:
                col_inds = range(0,len(temp_col))

            for k in new_tup_list:
                printing.append(k)

            for l in col_inds:
                print k[l],
            print '\n'


    def process_where_join(self,cols,dbs,where_clause,schema):
        print 'processing where join'
        flag = 0
        temp_col = []
        join_cols = []
        tup = []
        new_tup_list = []
        for i in dbs:
            for k in schema.table_list:
                if i == k.name and flag==0:
                    temp_col = temp_col+k.col_list
                    join_cols.append(k.col_list)
                    tup.append(k.tuple_list)
        col = temp_col
        for i in tup[1]:
            for j in tup[0]:#IF COND ONLY THEN ADD TO TEMP TABLE
                g = j+i
                new_tup_list.append(tuple(g))
        temp_table = Table('temp',temp_col)
        temp_table.tuple_list = new_tup_list

        AND = 0
        OR = 0
        if 'and' in where_clause or 'AND' in where_clause:
            where1 = where_clause[1].strip('\n')
            where2 = where_clause[4].strip('\n')
            AND = 1
        elif 'or' in where_clause or 'OR' in where_clause:
            where1 = where_clause[1].strip('\n')
            where2 = where_clause[4].strip('\n')
            OR = 1
        else:
            where1 = where_clause[1].strip('\n')
            where2 = ''

        cond1_value =''
        cond1_table = ''
        cond2_value =''
        cond2_table = ''

        if '<=' in where1:
            cond1_table = where1.split('<=')[0]
            cond1_value = where1.split('<=')[1]
            op1=4
        elif '>=' in where1:
            cond1_table = where1.split('>=')[0]
            cond1_value = where1.split('>=')[1]
            op1=5
        elif '<' in where1:
            cond1_table = where1.split('<')[0]
            cond1_value = where1.split('<')[1]
            op1 = 1
        elif '>' in where1:
            cond1_table = where1.split('>')[0]
            cond1_value = where1.split('>')[1]
            op1=2
        elif '=' in where1:
            cond1_table = where1.split('=')[0]
            cond1_value = where1.split('=')[1]
            op1 =3
        if '<=' in where2:
            cond2_table = where2.split('<=')[0]
            cond2_value = where2.split('<=')[1]
            op1=4
        elif '>=' in where2:
            cond2_table = where2.split('>=')[0]
            cond2_value = where2.split('>=')[1]
            op1=5
        elif '<' in where2:
            cond2_table = where2.split('<')[0]
            cond2_value = where2.split('<')[1]
            op2 =1
        elif '>' in where2:
            cond2_table = where2.split('>')[0]
            cond2_value = where2.split('>')[1]
            op2=2
        elif '=' in where2:
            cond2_table = where2.split('=')[0]
            cond2_value = where2.split('=')[1]
            op2=3

	if cols!=['*']:
            col_inds = [col.index(i) for i in cols]
        else:
            col_inds = range(0,len(col))

        ind1 = col.index(cond1_table)
        if cond2_table!='':
            ind2 = col.index(cond2_table)

        w1 = False
        w2 = False
        printing_list = []
        pr = []
        if AND==1:
            for k in new_tup_list:
                if op1==1 and int(k[ind1])<int(cond1_value):
                    w1 = True
                elif op1==2 and int(k[ind1])>int(cond1_value):
                    w1 = True
                elif op1==3 and int(k[ind1])==int(cond1_value):
                    w1 = True
                elif op1==4 and int(k[ind1])<=int(cond1_value):
                    w1 = True
                elif op1==5 and int(k[ind1])>=int(cond1_value):
                    w1 = True
                else:
                    w1 = False

                if op2==1 and int(k[ind2])<int(cond2_value):
                    w2 = True
                elif op2==2 and int(k[ind2])>int(cond2_value):
                    w2 = True
                elif op2==3 and int(k[ind2])==int(cond2_value):
                    w2 = True
                elif op2==4 and int(k[ind2])<=int(cond2_value):
                    w2 = True
                elif op2==5 and int(k[ind2])>=int(cond2_value):
                    w2 = True
                else:
                    w2 = False

                if (w1 and w2):
                    for s in col_inds:
                        printing_list.append(k[s])
                    wow = tuple(printing_list)
                    printing_list = []
                    pr.append(wow)

        elif OR==1:
            for k in new_tup_list:
                if op1==1 and int(k[ind1])<int(cond1_value):
                    w1 = True
                elif op1==2 and int(k[ind1])>int(cond1_value):
                    w1 = True
                elif op1==3 and int(k[ind1])==int(cond1_value):
                    w1 = True
                elif op1==4 and int(k[ind1])<=int(cond1_value):
                    w1 = True
                elif op1==5 and int(k[ind1])>=int(cond1_value):
                    w1 = True
                else:
                    w1 = False

                if op2==1 and int(k[ind2])<int(cond2_value):
                    w2 = True
                elif op2==2 and int(k[ind2])>int(cond2_value):
                    w2 = True
                elif op2==3 and int(k[ind2])==int(cond2_value):
                    w2 = True
                elif op2==4 and int(k[ind2])<=int(cond2_value):
                    w2 = True
                elif op2==5 and int(k[ind2])>=int(cond2_value):
                    w2 = True
                else:
                    w2 = False

                if (w1 or w2):
                    for s in col_inds:
                        printing_list.append(k[s])
                    wow = tuple(printing_list)
                    printing_list = []
                    pr.append(wow)
        pr = list(set(pr))
        for i in cols:
            print i,
        print '\n'
        for sou in pr:
            for jan in sou:
                print jan,
            print '\n'


    def select(self, split_exp,schema):
        list_keywords = [str(i) for i in split_exp if str(i)!=' ']
        curr_table_list = []
        tabe_names = []
        where_clause = ''
        cols = [i.strip('\n').strip(' ') for i in list_keywords[1].split(',')]
        dbs = [i.strip('\n').strip(' ') for i in list_keywords[4].split(',')]
        for i in schema.table_list:
            tabe_names.append(i.name)
        if len(list_keywords)>=7:
            where_clause = list_keywords[6].split(' ')
        if len(dbs)==1 and where_clause=='':
            if dbs[0] not in tabe_names:
                print 'Error: no table with name: ' + dbs[0]
            if cols[0]=='*':
                if where_clause=='':
                    a = [i for i in schema.table_list if i.name==dbs[0]]
                    for j in a:
                        j.display_table('all',schema,0,0,0,0,0,0)
            else:
                flag1 = 0
                for l in cols:
                    if 'sum' in l:
                        a = [i for i in schema.table_list if i.name==dbs[0]]
                        if '(' in l:
                            new_cols = l.split('(')[1].split(')')[0]
                        else:
                            new_cols = l
                        for j in a:
                            j.display_table(new_cols,schema,1,0,0,0,0,0)
                    elif 'avg' in l:
                        a = [i for i in schema.table_list if i.name==dbs[0]]
                        if '(' in l:
                            new_cols = l.split('(')[1].split(')')[0]
                        else:
                            new_cols = l
                        for j in a:
                            j.display_table(new_cols,schema,0,1,0,0,0,0)
                    elif 'max' in l:
                        a = [i for i in schema.table_list if i.name==dbs[0]]
                        if '(' in l:
                            new_cols = l.split('(')[1].split(')')[0]
                        else:
                            new_cols = l
                        for j in a:
                            j.display_table(new_cols,schema,0,0,1,0,0,0)
                    elif 'min' in l:
                        a = [i for i in schema.table_list if i.name==dbs[0]]
                        if '(' in l:
                            new_cols = l.split('(')[1].split(')')[0]
                        else:
                            new_cols = l
                        for j in a:
                            j.display_table(new_cols,schema,0,0,0,1,0,0)
                    elif 'distinct' in l:
                        a = [i for i in schema.table_list if i.name==dbs[0]]
                        if '(' in l:
                            new_cols = l.split('(')[1].split(')')[0]
                        else:
                            new_cols = l
                        for j in a:
                            j.display_table(new_cols,schema,0,0,0,0,1,0)
                    elif 'count' in l:
                        a = [i for i in schema.table_list if i.name==dbs[0]]
                        if '(' in l:
                            new_cols = l.split('(')[1].split(')')[0]
                        else:
                            new_cols = l
                        for j in a:
                            j.display_table(new_cols,schema,0,0,0,0,0,1)

                    else:
                        flag1 = 1
                if flag1==1:
                    a = [i for i in schema.table_list if i.name==dbs[0]]
                    for j in a:
                        j.display_table(cols,schema,0,0,0,0,0,0)

        elif len(dbs)==1 and where_clause!='':
            if dbs[0] not in tabe_names:
                print 'Error: no table with name: ' + dbs[0]
            where_clause = [i for i in where_clause if i!='where']
            self.process_where(cols,dbs,where_clause,schema)
        elif len(dbs)==2:
            flag = 0
            for i in dbs:
                if i not in tabe_names:
                    flag = 1
                    print 'Error: no table with name: ' + i
            if flag==0:
                if where_clause=='':
                    self.process_join(cols,dbs,schema)
                else:
                    self.process_where_join(cols,dbs,where_clause,schema)
        else:
            print 'Error: only two tables join allowed.'

    def parse(self,query_inst,schema):
        cmds = sqlparse.split(self.text)
        for smt in cmds:
            exp_type = sqlparse.parse(sqlparse.format(str(smt), reindent=True))[0]
            split_exp = exp_type.tokens
            split_exp_key = str(split_exp[0])
            if split_exp_key=='quit' or split_exp_key=='QUIT':
                sys.exit(0)
            elif split_exp_key=='select' or split_exp_key=='SELECT':
                Query.select(query_inst,split_exp,schema)
            else:
                print('Error: Only select queries allowed. Please try again.')

class Schema:
    def __init__(self,name):
        self.name = name
        self.table_list = []

    def read_metadata(self,filename):
        f = open(filename, 'r')
        a = f.read()
        a = a.split('\r\n')
        table = []
        for i in range(0,len(a)):
            #print(i,a[i])
            if(a[i]=='<begin_table>'):
                flag = 1
            elif a[i]=='<end_table>':
                flag = 0

            if flag==1 and a[i]!='<begin_table>':
                table.append(a[i])
            elif flag==0 and table!=[]:
                ro_list = [table[i] for i in range(1,len(table))]
                t = Table(table[0],ro_list)
                self.table_list.append(t)
                table = []

    def display_db(self):
        for i in self.table_list:
            i.display_table()


    def read_data_tables(self):
        for i in self.table_list:
            i.read_data()



schema = Schema('database')
schema.read_metadata('files/metadata.txt')#raw_input('Enter the metadata filepath:'))
schema.read_data_tables()
#schema.display_db()

while True:
    a = raw_input('sousql>')
    query = Query(a)
    query.parse(query, schema)
