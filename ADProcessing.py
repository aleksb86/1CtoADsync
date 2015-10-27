#-*- coding: utf-8 -*-

# import needed modules
import ldap
import ldap.modlist as modlist
import psycopg2
import datetime
import sys

class ADProcessing:
    sPrincipal = "%s@KURSKSMU.NET"
    sDN = 'cn=%s,%s'
    fName = '/home/dev/%s.csv'%datetime.datetime.now().strftime('%Y_%m')
    pg_conn_string = "host='{0}' dbname='postgres' user='{1}' password='{2}'"
    domenDN = 'dc=kursksmu,dc=net'

    ###############################
    # user processing
    userOU = 'ou=SyncUsers,dc=KURSKSMU,dc=NET'

    insUserQuery = "INSERT INTO personal.person_san(san, person_id) VALUES (%s, %s);"
    userAddQuery = '''
-------------------------------------------------------------------------------------------------------------------
SELECT pn.id
     , TRIM(description) "displayName"
     , REGEXP_REPLACE(TRIM(description), '([^\\s]*) .*', '\\1') sn
     , TRIM(REGEXP_REPLACE(TRIM(description), '[^\\s]* (.*)', '\\1')) "givenName"
     , TRIM(description)
       || REGEXP_REPLACE(to_char((SELECT CASE WHEN COUNT(*) = 0 THEN 0 ELSE COUNT(*)+1 END FROM personal.ad_users au
                    WHERE LOWER(REPLACE(au.uname, '[ 0-9]', '')) = LOWER(REGEXP_REPLACE(TRIM(description), '[ 0-9]', '')) LIMIT 1)
                 , '9'), '\\s[0]{0,1}', '') cn
     , REGEXP_REPLACE(TRIM(description), '([^\\s]*)[\\s]*([^\\s])[^\\s]*[\\s]*([^\\s])[^\\s]*.*', '\\1\\2\\3')
       || REGEXP_REPLACE(to_char((SELECT CASE WHEN COUNT(*) = 0 THEN 0 ELSE COUNT(*)+1 END FROM personal.ad_users au
                    WHERE LOWER(REPLACE(au.san, '[ 0-9]', '')) = LOWER(REGEXP_REPLACE(TRIM(description), '([^\\s]*)[\\s]*([^\\s])[^\\s]*[\\s]*([^\\s])[^\\s]*.*', '\\1\\2\\3')) LIMIT 1)
                 , '9'), '\\s[0]{0,1}', '') "sAMAccountName"
  FROM personal.persons pn
 WHERE pn.id IN (
                SELECT em.person_id
                  FROM personal.employees em
                 WHERE (em.leave_date IS NULL OR current_date BETWEEN em.recept_date AND em.leave_date)
                   AND em.person_id NOT IN (
                                    SELECT person_id
                                      FROM personal.person_san)
                   AND em.current_post_id IN ('000000035', '000000368', '000000216', '000000217', '000000346', '000000050', '000000383', '000000041', '000000019', '000000265', '000000005', '000000336', '000000114', '000000074', '000000347', '000000016', '000000128', '000000117', '000000105', '000000158', '000000109', '000000365', '000000099', '000000054', '000000381', '000000382', '000000183', '000000088', '000000340', '000000059', '000000125', '000000221', '000000056', '000000327', '000000328', '000000329', '000000077', '000000359', '000000358', '000000332', '000000091', '000000273', '000000296', '000000206', '000000159', '000000130', '000000160', '000000113', '000000100', '000000083', '000000254', '000000132', '000000052', '000000142', '000000004', '000000108', '000000133', '000000126', '000000098', '000000070', '000000342', '000000250', '000000062', '000000251', '000000127', '000000053', '000000385', '000000055', '000000134', '000000356', '000000089', '000000111', '000000095', '000000051', '000000026', '000000092', '000000338', '000000371', '000000085', '000000191', '000000335', '000000096', '000000068', '000000038', '000000023', '000000090', '000000079', '000000058', '000000057', '000000135', '000000086')
                 GROUP
                    BY em.person_id
                )
 ORDER BY "sAMAccountName"
-------------------------------------------------------------------------------------------------------------------
    '''

    class ksmuAdUserAttributes:

        SCRIPT = 1
        ACCOUNTDISABLE = 2
        HOMEDIR_REQUIRED = 8
        PASSWD_NOTREQD = 32
        NORMAL_ACCOUNT = 512
        DONT_EXPIRE_PASSWORD = 65536
        TRUSTED_FOR_DELEGATION = 524288
        PASSWORD_EXPIRED = 8388608

        def __init__(self, record, attrNames):
            self.id = record[0]
            self.attrs = {}
            for i, aName in enumerate(attrNames[1:]):
                self.attrs[aName] = record[i+1]
            self.attrs['objectclass'] = ['top','person','organizationalPerson','user']
            self.attrs['userPrincipalName'] = ADProcessing.sPrincipal % self.attrs['sAMAccountName']
            # Some flags for userAccountControl property
            self.attrs['userAccountControl'] = str(ADProcessing.ksmuAdUserAttributes.NORMAL_ACCOUNT
                                                   + ADProcessing.ksmuAdUserAttributes.ACCOUNTDISABLE)
            self.dn = ADProcessing.sDN % (self.attrs['cn'], ADProcessing.userOU)

    def userProcessing(self):
        cursor = self.conn.cursor()
        cursor.execute(ADProcessing.userAddQuery)
        attrNames = [i[0] for i in cursor.description]
        records = cursor.fetchall()
        for rc in records:
            adAttrs = ADProcessing.ksmuAdUserAttributes(rc, attrNames)
            ldif = modlist.addModlist(adAttrs.attrs)
            try:
                self.l.add_s(adAttrs.dn,ldif)
                # add to table 'person_san'
                self.addToPersonSan(cursor, (adAttrs.attrs['sAMAccountName'], adAttrs.id))
                # add to array for csv
                self.addUserToCSV(adAttrs.dn, adAttrs.attrs)
            except:
                pass

    def addToPersonSan(self, cursor, data):
        cursor.execute(ADProcessing.insUserQuery, data)
        self.conn.commit()

    def addUserToCSV(self, dn, attrs):
        line = '"%s"'%dn
        for nm in attrs.keys():
            line = line + ';"%s"'%attrs[nm]
        self.f.write(line+'\r\n')

    ##########################################
    # depts Processing
    groupOU = 'OU=SyncGroups,OU=SecurityGroups,DC=kursksmu,DC=net'

    groupModQuery = '''
SELECT ag.dn dn
     , dm.description description
  FROM personal.ad_groups ag
     , personal.department_san ds
     , personal.departments dm
 WHERE ds.department_id = dm.id
   AND ag.san = ds.san
   AND NOT LOWER(REPLACE(dm.description, ' ', '')) = LOWER(REPLACE(ag.gname, ' ', ''))
 UNION ALL
SELECT ag.dn
     , pt.description
  FROM personal.ad_groups ag
     , personal.post_san ps
     , personal.posts pt
 WHERE ps.post_id = pt.id
   AND ag.san = ps.san
   AND NOT LOWER(REPLACE(pt.description, ' ', '')) = LOWER(REPLACE(ag.gname, ' ', ''))
    '''

    class ksmuAdGroupAttributes:
        def __init__(self, record, ounit, prefix):
            self.attrs = {}
            self.attrs['objectclass'] = ['top','group']
            self.id = record[0]
            self.attrs['sAMAccountName'] = prefix+self.id
            self.attrs['cn'] = prefix+self.id
            self.attrs['name'] = prefix+self.id
            desc = record[1]
            self.attrs['displayName'] = desc
            self.attrs['description'] = desc
            self.dn = ADProcessing.sDN % (self.attrs['cn'], ounit)

    def groupProcessing(self):
        #groups
        self.modGroupName()

        # departments
        self.addDept()
        self.modDeptStuff()

        # posts
        self.addPost()
        self.modPostStuff()

    def modifyTheGroupStuff(self, dn, members):
            try:
                ad_group = self.l.search_s(dn, ldap.SCOPE_BASE, '(objectclass=group)', ['member'])
                old_dct = ad_group[0][1].copy()
                new_dct = {'member':members}
                ldif = modlist.modifyModlist(old_dct, new_dct)
                self.l.modify_s(dn,ldif)
            except:
                pass

    def addGroup(self, addQuery, ounit, insQuery, prefix):
        self.cursor.execute(addQuery)
        records = self.cursor.fetchall()
        try:
            for rc in records:
                adAttrs = ADProcessing.ksmuAdGroupAttributes(rc, ounit, prefix)
                ldif = modlist.addModlist(adAttrs.attrs)
                print adAttrs.dn
                self.l.add_s(adAttrs.dn,ldif)
                # add to data base
                self.cursor.execute(insQuery, (adAttrs.attrs['sAMAccountName'], adAttrs.id))
                self.conn.commit()
        except:
            pass


    def modGroupName(self):
        cursor = self.conn.cursor()
        cursor.execute(self.groupModQuery)
        records = cursor.fetchall()
        for rc in records:
            dn, desc = rc[0], rc[1]
            ad_group = self.l.search_s(dn, ldap.SCOPE_BASE, '(objectclass=group)', ['displayName','description'])
            old_dct = ad_group[0][1].copy()
            new_dct = ad_group[0][1].copy()
            new_dct['displayName'] = desc
            new_dct['description'] = desc
            ldif = modlist.modifyModlist(old_dct, new_dct)
            try:
                self.l.modify_s(dn,ldif)
            except:
                pass

    def modGroupStuff(self, stuffQuery):
        self.cursor.execute(stuffQuery)
        records = self.cursor.fetchall()
        dn, members = None, []
        for rc in records:
            if dn != rc[0]:
                if dn != None:
                    self.modifyTheGroupStuff(dn, members)
                    members = []
            dn, mmb = rc[0], rc[1]
            if mmb != None:
                members.append(mmb)
        self.modifyTheGroupStuff(dn, members)

    #####################################
    deptOU = 'OU=Departments,OU=SyncGroups,OU=SecurityGroups,DC=kursksmu,DC=net'
    deptPrefix = 'dept'
    insDeptQuery = "INSERT INTO personal.department_san(san, department_id) VALUES (%s, %s);"
    deptAddQuery = '''
SELECT id
     , description
  FROM personal.departments
 WHERE id NOT IN (
                 SELECT department_id
                   FROM personal.department_san)
  AND id NOT IN ('000000188') -- department archive
    '''

    deptStuffQuery = '''
SELECT ag.dn
     , au.distinguished_name
  FROM (SELECT *
          FROM personal.ad_groups ag
         WHERE LOWER(ag.dn) LIKE LOWER('%OU=Departments,OU=SyncGroups,OU=SecurityGroups,DC=kursksmu,DC=net')
       ) ag
  LEFT JOIN
       (
       personal.department_san ds
       INNER JOIN
       personal.departments dt
       ON ds.department_id = dt.id
       INNER JOIN
       (SELECT * FROM personal.employees el
         WHERE (el.leave_date IS NULL OR current_date BETWEEN el.recept_date AND el.leave_date)) el
       ON dt.id = el.current_department_id
       INNER JOIN
       personal.person_san ps
       ON ps.person_id = el.person_id
       INNER JOIN
       personal.ad_users au
       ON au.san = ps.san
       )
    ON ag.san = ds.san
 GROUP
    BY ag.dn
     , au.distinguished_name
 ORDER
    BY ag.dn
     , au.distinguished_name
    '''

    def addDept(self):
        self.addGroup(self.deptAddQuery, self.deptOU, self.insDeptQuery, self.deptPrefix)

    def modDeptStuff(self):
        self.modGroupStuff(self.deptStuffQuery)

    ##########################################
    # posts Processing

    postOU = 'OU=Posts,OU=SyncGroups,OU=SecurityGroups,DC=kursksmu,DC=net'
    postPrefix = 'post'
    insPostQuery = "INSERT INTO personal.post_san(san, post_id) VALUES (%s, %s);"

    postAddQuery = '''
SELECT id
     , TRIM(description) description
  FROM personal.posts
 WHERE id NOT IN (
                 SELECT post_id
                   FROM personal.post_san);
    '''

    postStuffQuery = '''
SELECT ag.dn
     , au.distinguished_name
  FROM (SELECT *
          FROM personal.ad_groups ag
         WHERE LOWER(ag.dn) LIKE LOWER('%OU=Posts,OU=SyncGroups,OU=SecurityGroups,DC=kursksmu,DC=net')
       ) ag
  LEFT JOIN
       (
       personal.post_san pts
       INNER JOIN
       personal.posts pt
       ON pts.post_id = pt.id
       INNER JOIN
       (SELECT * FROM personal.employees el
         WHERE (el.leave_date IS NULL OR current_date BETWEEN el.recept_date AND el.leave_date)) el
       ON pt.id = el.current_post_id
       INNER JOIN
       personal.person_san ps
       ON ps.person_id = el.person_id
       INNER JOIN
       personal.ad_users au
       ON au.san = ps.san
       )
    ON ag.san = pts.san
 GROUP
    BY ag.dn
     , au.distinguished_name
 ORDER
    BY ag.dn
     , au.distinguished_name

    '''

    def addPost(self):
        self.addGroup(self.postAddQuery, self.postOU, self.insPostQuery, self.postPrefix)

    def modPostStuff(self):
        self.modGroupStuff(self.postStuffQuery)

#####################################################################################################################
#####################################################################################################################
    # getter from AD to pgSQL

    class getterAdData():

        insAdGroupQuery = "INSERT INTO personal.ad_groups(gname, san, dn, cn) VALUES(%s, %s, %s, %s)"
        insAdUserQuery = 'INSERT INTO personal.ad_users(uname,san,mail,cn,display_name,given_name,sn,upn,distinguished_name)' \
                         +                      'VALUES(%s   ,%s ,%s  ,%s,%s          ,%s        ,%s,%s ,%s)'

        truncateAdGroupQuery = 'TRUNCATE TABLE personal.ad_groups;'
        truncateAdUserQuery = 'TRUNCATE TABLE personal.ad_users;'

        def to_file(self, out_file, obj_list):
            res_file = open(out_file, 'a')
            for el in obj_list:
                res_file.write(el + '\n')
            res_file.close()

        def get_ou_list(self):
            ou_search_set = self.conn.search_s( ADProcessing.domenDN, ldap.SCOPE_ONELEVEL,
                                                '(objectClass=organizationalUnit)',
                                                ['ou'])
            return [ou[0] for ou in ou_search_set if ou[0] is not None]

        uAttrNames = ['name',
                     'sAMAccountName',
                     'mail',
                     'cn',
                     'displayName',
                     'givenName',
                     'sn',
                     'userPrincipalName',
                     'distinguishedName']
        def get_usr_list(self, ou_name):
            usr_search_set = self.conn.search_s( ou_name, ldap.SCOPE_SUBTREE,
                                                 '(objectClass=user)',
                                                 self.uAttrNames)
            queryDatas = [ [  usr[1][attrName][0] if attrName in usr[1] else '' for attrName in self.uAttrNames]
                      for usr in usr_search_set]

            for queryData in queryDatas:
                self.cursor.execute(self.insAdUserQuery, queryData)

        gAttrNames = ['displayName',
                     'sAMAccountName',
                     'distinguishedName',
                     'cn']
        def get_group_list(self):
            grp_search_set = self.conn.search_s(ADProcessing.groupOU, ldap.SCOPE_SUBTREE,
                                                 '(objectCategory=group)',
                                                 self.gAttrNames)
            queryDatas = [[grp[1][aName][0] for aName in self.gAttrNames]
                    for grp in grp_search_set]

            for queryData in queryDatas:
                self.cursor.execute(self.insAdGroupQuery, queryData)

        def __init__(self, conn, dbconn):
            self.conn, self.cursor = conn, dbconn.cursor()
            #try:
            # sinc groups
            self.cursor.execute(self.truncateAdGroupQuery)
            self.get_group_list()
            # sinc users
            self.cursor.execute(self.truncateAdUserQuery)
            for ou in self.get_ou_list():
               self.get_usr_list(ou)
            dbconn.commit()
            #except:
            #    pass

#####################################################################################################################
#####################################################################################################################
    # Connector to AD by LDAP

    iLDAP_URI = 'ldapuri'
    iLDAP_USR = 'ldaplogin'
    iLDAP_PWD = 'ldappw'

    class ADInit():
        conn = None
        def __init__(self, ldapuri, usr, pw):
            self.ldapuri, self.usr, self.pw = ldapuri, usr, pw

        def connect(self):
            try:
                self.conn = ldap.initialize(self.ldapuri)
                return self.conn
            except ldap.LDAPError, e:
                pass

        def ad_bind(self):
            try:
                self.conn.set_option(ldap.OPT_REFERRALS, 0)
                self.conn.simple_bind_s(self.usr, self.pw)
            except ldap.LDAPError, e:
                pass

        def disconnect(self):
            try:
                self.conn.unbind_s()
            except ldap.LDAPError, e:
                pass

######################################################################################################################

    iPgSQL_IP  = 'dbip'
    iPgSQL_USR = 'dbuser'
    iPgSQL_PWD = 'dbpw'

    def __init__(self, argvDct):
        # Open a connection
        self.adcon = ADProcessing.ADInit(argvDct[self.iLDAP_URI],argvDct[self.iLDAP_USR],argvDct[self.iLDAP_PWD])
        self.l = self.adcon.connect()
        self.adcon.ad_bind()


        # get a connection, if a connect cannot be made an exception will be raised here
        self.conn = psycopg2.connect(ADProcessing.pg_conn_string.format(
                                             argvDct[self.iPgSQL_IP],argvDct[self.iPgSQL_USR],argvDct[self.iPgSQL_PWD]))
        self.cursor = self.conn.cursor()
        self.f = open(ADProcessing.fName, 'a')

        ADProcessing.getterAdData(self.l, self.conn)
        self.userProcessing()
        self.groupProcessing()
        ADProcessing.getterAdData(self.l, self.conn)

        # Its nice to the server to disconnect and free resources when done
        self.adcon.disconnect()
        self.f.close()



##########################################################################################################
##########################################################################################################
##########################################################################################################

# argvDct['ldaplogin'], argvDct['ldappw']     --   for LDAP connect to AD
# argvDct['dbuser'], argvDct['dbpw']          --   for connect to PgSQL
if __name__ == "__main__":
    ADProcessing(dict(   zip(  [s[1:] for s in sys.argv[1::2] ],  sys.argv[2::2]  )   ))

