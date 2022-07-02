import html
import re
class Scrape:
    def DbGetRecords(query,db_connection,count=""):
        try:db_cursor = db_connection.cursor(dictionary=True)
        except:
           db_connection.reconnect(attempts=3, delay=0.3)
           db_cursor = db_connection.cursor(dictionary=True)
        db_cursor.execute(query)
        records = db_cursor.fetchall()
        if count==1:
            return len(records)
        else:
            return records
    def test():
            print ("dfasdf  fdsaf afd afdasf ")
    def download_url(url, save_path, chunk_size=128):
        r = requests.get(url, stream=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)        
    def DbInsertSql(tbl,a_dict):        
        Query      = "UPDATE "+tbl+" ("
        Values     = " SET ("
        for k, v in a_dict.items():
            # if v == '': 
                # v  = None
            # else:
                # v = v.strip()
            ##if v !=None: v = v.strip()
            a_dict[k] = v
            Query=Query+"`"+k+"`,"
            Values=Values+"%("+k+")s,"        
        sql=Query[:-1]+") "+Values[:-1]+")"
        return  sql
    def ArrSql(a_dict,tbl=""):
        r=1
        print("CREATE TABLE `"+tbl+"` (")		
        for k, v in a_dict.items():
            print ("`"+k+"` varchar(128) DEFAULT NULL,\n")
        print(') ENGINE=MyISAM DEFAULT CHARSET=utf8;')
        exit; 
    def companySearch(a):
        result = {}
        o=a        
        findme=[' to the order of ',' the order of ',' to order of ',' of:',' of=',' of,',' of. ','to the order ','to order ',' the ','a series of','/de/','\de\\','/md/','/new/','/nv/','/oh/','(de)','(cayman)','/adr','/de','&amp;','&amp','&','/adv','/ca',' and ','/ fa ','/fa/',' / adr ','(the)','/fi','/the','-adr','on behalf of','dba']    
        for key in findme:
            a=html.unescape(" "+a.lower()+" ").replace(key, " ")
        a=re.sub('/[^\da-z]/i'," ",a).strip()
        shortName={'care of':'c o','corporation':'corp','pllc':'p l l c','pc':'p c','plc':'p l c','imc':'inc','incorpoarted':'inc','incorporated':'inc','incorporation':'inc','limited liability company':'l l c','llc':'l l c','limited partnership':'l p','lp':'l p','limited':'l t d','limiteda':'l t d','ltda':'l t d a','ltd':'l t d','gmbh':'g m b h','company':'co','copmany':'co','cooperative':'co','bv':'b v','aps':'a p s','nv':'n v','spa':'s p a','sas':'s a s','sa':'s a','as':'a s','anonim sirketi':'a s','srl':'s r l','enraf':'e n r a f','sro':'s r o','bv':'b v','of north america':'','of america':'','intl':'i n t','int l':'i n t','international':'i n t','internation':'i n t','int':'i n t'}
        for k, v in shortName.items():
            a=  a.replace(" "+k+" ", " "+v+" ")
        a   =   a.strip()
        n   =   a;
        a   =   a.strip().replace(' ','%')+'%';
        result[0]=n
        result[1]=a
        result[2]=o
        return result
    def DbInsert(tbl,a_dict,db_connection):
        try:db_cursor = db_connection.cursor(dictionary=True)
        except:
           db_connection.reconnect(attempts=3, delay=0.3)
           db_cursor = db_connection.cursor(dictionary=True)
        Query      = "REPLACE INTO "+tbl+" ("        
        Values     = " VALUES ("
        for k, v in a_dict.items():            
            a_dict[k] = v
            Query=Query+"`"+k+"`,"
            Values=Values+"%("+k+")s,"        
        sql=Query[:-1]+") "+Values[:-1]+")"        
        try:
            db_cursor.execute(sql, a_dict)
            db_connection.commit()
            return db_cursor.lastrowid
        except Exception as e:
            pprint (e)
            pprint(a_dict)
            exit()
    def InsertIgnoreDb(tbl,a_dict,db_connection):
        db_cursor = db_connection.cursor()
        Query      = "INSERT IGNORE "+tbl+" ("
        Values     = " VALUES ("
        for k, v in a_dict.items():
            Query=Query+"`"+k+"`,"
            Values=Values+"%("+k+")s,"
        sql=Query[:-1]+") "+Values[:-1]+")"
        try:
            db_cursor.execute(sql, a_dict)
            db_connection.commit()
            return db_cursor.lastrowid
        except e:
            print (e)
            exit()   
