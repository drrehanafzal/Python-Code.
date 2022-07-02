from pprint import pprint
from libs.Lib import Scrape as obj
from DB.Db import db_connection
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
import time
from dateutil.relativedelta import relativedelta
date_after_month    =   datetime.today()+ relativedelta(months=-1)
endDate             =   datetime.today().strftime('%Y-%m-%d')+" 00:00:00"
startDate           =   date_after_month.strftime('%Y-%m-%d')+" 00:00:00"
sql1                =   "SELECT `merchant_id` FROM `masterdata_prediction` WHERE `created_date`>='"+str(startDate)+"' AND `created_date`<='"+str(endDate)+"' AND `status`='BILLED' GROUP BY `merchant_id`"
records             =   obj.DbGetRecords(sql1,db_connection)
for row in records:
    for index in range(0,31):
        new_datetime        = datetime.strptime(date_after_month.strftime('%Y%m%d'), '%Y%m%d')
        new_datetime        = new_datetime + timedelta(days=index)
        queryDate           = datetime.strptime(new_datetime.strftime('%Y%m%d'), "%Y%m%d")
        lastMonthStartDate  = datetime.strptime(date_after_month.strftime('%Y%m%d'), "%Y%m%d")
        MonthEndDate        = datetime.strptime(datetime.today().strftime('%Y%m%d'), "%Y%m%d")
        if queryDate >= lastMonthStartDate and queryDate<=MonthEndDate:
            sql2="SELECT `id`,`transaction_origination_country`,`currency_code`,`status`,`merchant_id`,`execution_type`,`price_point`,`created_date`,sum(`price_point`) as `predict`,count('*') as `total_transaction` FROM `masterdata_prediction` WHERE `status`='BILLED' AND `created_date`>='"+str(datetime.strptime(queryDate.strftime('%Y-%m-%d'), "%Y-%m-%d"))+"' AND `created_date`<='"+str(datetime.strptime(queryDate.strftime('%Y-%m-%d'), "%Y-%m-%d")).replace("00:00:00","23:59:59")+"' AND `merchant_id`='"+str(row['merchant_id'])+"'"
            returnCount=obj.DbGetRecords(sql2,db_connection)
            try:
                if returnCount[0]['predict'] is not None:
                    #print(sql2)
                    #exit()
                    a_dict  = dict()
                    a_dict['country']               =  returnCount[0]['transaction_origination_country']
                    a_dict_day                      =  datetime.strptime(queryDate.strftime('%Y%m%d'), '%Y%m%d')
                    a_dict_day                      =  a_dict_day + relativedelta(days=30)
                    a_dict_day                      =  a_dict_day.strftime('%Y-%m-%d')
                    a_dict['date']                   =  a_dict_day
                    #a_dict['queryDate']             =  queryDate
                    a_dict['price']                 =  "{:.2f}".format(returnCount[0]['predict'])
                    a_dict['currency']              =  returnCount[0]['currency_code']
                    a_dict['status']                =  returnCount[0]['status']
                    a_dict['type']                  =  returnCount[0]['execution_type']
                    a_dict['total_transaction']     =  returnCount[0]['total_transaction']
                    a_dict['merchant_id']           =   row['merchant_id']
                    obj.DbInsert('tbl_transaction_prediction',a_dict,db_connection)
                    print("Save....");
            except Exception as e:
                print(e)
        
