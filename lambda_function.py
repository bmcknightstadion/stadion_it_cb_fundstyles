import DBUtilMySQL as db
import boto3
import json

def archiveFundStyleDetail(asOfDt):
    rc = -1
    count = 0
    msg = ""
    conn = db.get_db("ned2")
    rows = conn.cursor().execute("select count(*) as c from fund_style_detail where as_of_dt='"+str(asOfDt)+"'")
    for r in rows:
        count = int(r[0])
    if count == 0:
        msg = "no runs in fund_style_detail for "+str(asOfDt)
        print(msg)
        return -11,msg
    conn.execute("delete from fund_style_detail_archive where as_of_dt='"+str(asOfDt)+"'")
    
    sql = """insert into fund_style_detail_archive (cusip, as_of_dt, asset_class_id, percentage)
        select cusip, as_of_dt, asset_class_id, percentage
        from fund_style_detail where as_of_dt='"""+str(asOfDt)+"""'
        """
    print(sql)
    conn.execute(sql)
    conn.execute("delete from fund_style_detail where as_of_dt='"+str(asOfDt)+"'")
    conn.commit()
    conn.close()
    rc = 0
    msg = str(count)+" rows archived"
    return rc,msg

def archiveFundStyle(asOfDt):
    rc = -1
    msg = ""
    count = 0
    conn = db.get_db("ned2")
    rows = conn.cursor().execute("select count(*) as c from fund_style where as_of_dt='"+str(asOfDt)+"'")
    for r in rows:
        count = int(r[0])
    if count == 0:
        msg = "no runs in fund_style for "+str(asOfDt)
        print(msg)
        return -11,msg
    conn.execute("delete from fund_style_archive where as_of_dt='"+str(asOfDt)+"'")
    
    sql = """insert into fund_style_archive (cusip, as_of_dt, name, fund_type, mfid, ret_1yr, ret_3yr, ret_5yr, sd_3yr, large_cap_rollup, small_cap_rollup, intl_rollup, bond_rollup, r_squared, expense_ratio, num_periods, ticker)
        select cusip, as_of_dt, name, fund_type, mfid, ret_1yr, ret_3yr, ret_5yr, sd_3yr, large_cap_rollup, small_cap_rollup, intl_rollup, bond_rollup, r_squared, expense_ratio, num_periods, ticker
        from fund_style where as_of_dt='"""+str(asOfDt)+"""'
        """
    print(sql)
    conn.execute(sql)
    conn.execute("delete from fund_style where as_of_dt='"+str(asOfDt)+"'")
    conn.commit()
    conn.close()
    rc = 0
    msg = str(count)+" rows archived"
    return rc,msg
def clean(newD):
    val = "'"+str(newD).replace("\n","").replace("\r","").replace("\"\"","~").replace("\"","").replace("~","\"").replace("'","''")+"'"
    return ''.join(char for char in val if ord(char) < 128)

def loadFile(asOfDt):
    rc = 0
    msg = ""
    s3_client = boto3.client('s3')
    fileName = "master_style_table-Valic-"+str(asOfDt)+".csv"
    s3_client.download_file('stadion-cb.proc-data', 'corebridge/mst/'+str(fileName), '/tmp/'+str(fileName))
    conn = db.get_db("ned2")
    conn.execute("delete from fund_style where as_of_dt='"+str(asOfDt)+"'")
    conn.execute("delete from fund_style_detail where as_of_dt='"+str(asOfDt)+"'")
    conn.commit()
    acDict = {}
    rows = conn.cursor().execute("select asset_class_id, asset_class_name from generic2.ref_asset_class")
    for r in rows:
        id = int(r[0])
        ac = str(r[1])
        acDict[ac] = id
    lines = 0
    file = open('/tmp/'+fileName,"rb")
    header = [
        "Cusip",        "unknown",        "Name",        "Type",        "Large Cap",        "Large Cap Growth",        "Large Cap Value",        "Mid Cap",
        "Small/Mid Cap",        "Small Cap",        "International",        "Emerging Markets",        "REIT",        "High Yield",
        "Bonds",        "Long Term Bonds",        "Short Term Bonds",        "Cash",        "Large Cap Individual",        "Mid Cap Individual",
        "Small Cap Individual",        "Short Term Muni",        "Muni Cash",        "Long Term Muni",        "Small Cap Growth",
        "Small Cap Value",        "TIPS",        "Direct Real Estate",        "Small/Mid Cap Growth",        "Small/Mid Cap Value",
        "Mid Cap Growth",        "Mid Cap Value",        "Commodities",        "Foreign Bonds",        "MFID",
        "1-Year Ret",        "3-Year Ret",        "5-Year Ret",        "3-Year SD",        "Large-Cap Roll-Up",        "Small-Cap Roll-Up",
        "Int'l Rollup",        "Bond Rollup",        "r Squared",        "Expense Ratio",        "No.Periods",        "Ticker Symbol"
            ]
    sql1 = ""
    sql1Header = "insert into fund_style (cusip, as_of_dt, name, fund_type, mfid, ret_1yr, ret_3yr, ret_5yr, sd_3yr, large_cap_rollup, small_cap_rollup, intl_rollup, bond_rollup, r_squared, expense_ratio, num_periods, ticker) values "
    sql2 = ""
    sql2Header = "insert into fund_style_detail (cusip,as_of_dt,asset_class_id,percentage) values "
    for line in file.readlines():
        if lines == 0:
            pass
        else:
            data = []
            #print(line)
            t = str(line.decode('latin1')).split(",")
            i = 0
            for d in t:
                newD = d
                if newD is None or newD == "" or newD=="None" or newD=="-":
                    newD = "null"
                else:
                    newD = clean(newD)
                t[i] = newD
                i = i + 1
            Cusip = str(t[0])
            unknown = str(t[1])
            Name = str(t[2])
            Type = str(t[3])
            LargeCap = str(t[4])
            LargeCapGrowth = str(t[5])
            LargeCapValue = str(t[6])
            MidCap = str(t[7])
            SmallMidCap = str(t[8])
            SmallCap = str(t[9])
            International = str(t[10])
            EmergingMarkets = str(t[11])
            REIT = str(t[12])
            HighYield = str(t[13])
            Bonds = str(t[14])
            LongTermBonds = str(t[15])
            ShortTermBonds = str(t[16])
            Cash = str(t[17])
            LargeCapIndividual = str(t[18])
            MidCapIndividual = str(t[19])
            SmallCapIndividual = str(t[20])
            ShortTermMuni = str(t[21])
            MuniCash = str(t[22])
            LongTermMuni = str(t[23])
            SmallCapGrowth = str(t[24])
            SmallCapValuev = str(t[25])
            TIPS = str(t[26])
            DirectRealEstate = str(t[27])
            SmallMidCapGrowth = str(t[28])
            SmallMidCapValue = str(t[29])
            MidCapGrowth = str(t[30])
            MidCapValue = str(t[31])
            Commodities = str(t[32])
            ForeignBonds = str(t[33])
            MFID = str(t[34])
            YearRet1 = str(t[35])
            YearRet3 = str(t[36])
            YearRet5 = str(t[37])
            YearSD = str(t[38])
            LargeCapRollUp = str(t[39])
            SmallCapRollUp = str(t[40])
            IntlRollup = str(t[41])
            BondRollup = str(t[42])
            rSquared = str(t[43])
            ExpenseRatio = str(t[44])
            NoPeriods = str(t[45])
            TickerSymbol = str(t[46])
            
            sql1 = sql1 + """
                    (%s, '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),""" % (Cusip, asOfDt, Name, Type, Cusip, YearRet1, YearRet3, YearRet5, YearSD, LargeCapRollUp, SmallCapRollUp, IntlRollup, BondRollup, rSquared, ExpenseRatio, NoPeriods, TickerSymbol)

            for key  in acDict.keys():
                i = 0
                found = False
                for h in header:
                    if key==h:
                        found = True
                        break
                    i = i + 1
                if found == False:
                    print("KEY: ",key)
                value = t[i]
                id = acDict[key]
                if value is None or value=='' or value=='0' or value=="-" or value=="'0'":
                    pass
                else:
                    sql2 = sql2 + "(%s,'%s',%s,%s),"%(Cusip,asOfDt,id,value)

            if lines % 100 ==0:
                if sql1.endswith(","):
                    sql1 = sql1[:-1]
                sql = sql1Header + sql1
                #print(sql)
                conn.execute(sql)
                
                sql1 = ""
                if sql2.endswith(","):
                    sql2 = sql2[:-1]
                sql = sql2Header + sql2
                sql2 = ""
                #print(sql)
                conn.execute(sql)
                conn.commit()
        lines = lines + 1
    if sql1.endswith(","):
        sql1 = sql1[:-1]
        sql = sql1Header + sql1
        #print(sql)
        conn.execute(sql)
        
    if sql2.endswith(","):
        sql2 = sql2[:-1]
        sql = sql2Header + sql2
        #print(sql)
        conn.execute(sql)

    print("LINES READ ",lines)
    msg = "num inserts: "+str(lines-1)
    file.close()
        
    conn.commit()
    conn.close()
    return rc,msg

def insert(conn,header,tokens,acDict,asOfDt):
    print(tokens)

def lambda_handler(event, context):
    print (event)
    try:
        action = event['action']
    except:
        action = None
    rc = -10
    msg = "not a valid action "+str(action)
    if action=='fund_style_archive':
        asOfDt = event['as_of_dt']
        print("Archiving fund_style ",asOfDt)
        rc = archiveFundStyle(asOfDt)
    elif action=='fund_style_detail_archive':
        asOfDt = event['as_of_dt']
        print("Archiving fund_style_detail ",asOfDt)
        rc = archiveFundStyleDetail(asOfDt)
    elif action=='load':
        asOfDt = event['as_of_dt']
        rc,msg = loadFile(asOfDt)

    return {"rc":rc,"msg":msg}

