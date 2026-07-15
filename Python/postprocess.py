import pandas as pd
import requests
import io
import json
from datetime import datetime,date

#save timestamp to text file
with open('timestamp.txt', 'w') as f:
    f.write(datetime.now().strftime("%Y-%b-%d"))
    
update = True

if update:
    # set average price reference month
    avgpriceRefMonth=pd.Timestamp('2025-01-01 00:00:00')

    # starting reference point
    startref=pd.Timestamp('2020-01-01 00:00:00')

    #read in metadata
    meta = pd.read_csv('./metadata.csv',index_col=0,parse_dates=['ITEM_START'],date_format="%Y%m")

    # define a function to split a string at a certain occurance of a separator

    # https://stackoverflow.com/questions/36300158/split-text-after-the-second-occurrence-of-character
    def split(strng, sep, pos):
        strng = strng.split(sep)
        return sep.join(strng[:pos]), sep.join(strng[pos:])

    # read in unchained csv
    unchained = pd.read_csv('unchained.csv')

    #find the last month in the unchained file
    latestmonth=datetime.strptime(unchained.columns[-1],"%Y-%m-%d")

    # first get the data.json from the cpi items and prices page

    with requests.Session() as s:
        r=s.get("https://www.ons.gov.uk/economy/inflationandpriceindices/datasets/consumerpriceindicescpiandretailpricesindexrpiitemindicesandpricequotes/data",headers={'User-Agent': 'Mozilla/5.0'},verify=False)
        data = r.json()
        datasets = data['datasets']


    #go through the dataset and find the first one which doesn't contain the word framework, glossary or /pricequotes. The url includes pricesquotes so that slash is important. Save the index as the variable match  
    for i,dataset in enumerate(datasets):
        match = i
        if('framework' not in dataset['uri'] and 'glossary' not in dataset['uri'] and '/pricequotes' not in dataset['uri']):
            break
        
    #get the uri of the items dataset we want
    items = data['datasets'][match]['uri']
    print('dataset='+items)

    #get the month and year from the uri
    date=split(items,'consumptionsegmentindices',1)[1]
    print('the date from url:'+date)

    #parse it as a date
    itemmonth=datetime.strptime(date,"%B%Y")


    # check date to see if you need to download a file
    if(itemmonth!=latestmonth):
        print('month from indices is different to latest month in unchained csv')
        # download the file
        with requests.Session() as s:
            r=s.get("https://www.ons.gov.uk"+items+"/data",headers={'User-Agent': 'Mozilla/5.0'},verify=False)
            itemspage = r.json()
            csv = itemspage['downloads'][0]['file']
        
        # get the csv of the latest indices

        with requests.Session() as s:
            download = s.get("https://www.ons.gov.uk/file?uri="+items+"/"+csv,headers={'User-Agent': 'Mozilla/5.0'},verify=False)
            df=pd.read_csv(io.StringIO(download.content.decode('utf-8')))
            df.rename(columns={'CS_ID': 'ITEM_ID', 'CS_DESC': 'ITEM_DESC', 'CPI_INDEX': 'ALL_GM_INDEX', 'CPIH_WEIGHT': 'ITEM_WEIGHT'}, inplace=True)
            
        #get the index date which is the first cell
        index_date=df.iloc[0,0]

        # parse columns as dates in unchained
        # https://stackoverflow.com/questions/42472418/parse-file-headers-as-date-objects-in-python-pandas
        columns = {}
        for col in unchained.columns:
            try:
                columns[col] = datetime.strptime(str(col), "%Y-%m-%d")
            except ValueError:
                pass
        unchained.rename(columns=columns, inplace=True)
        
        #join it onto existing csv
        un=unchained.merge(df[['ITEM_ID','ALL_GM_INDEX']].rename(columns={"ALL_GM_INDEX": datetime.strptime(str(index_date),"%Y%m")}),on='ITEM_ID',how='left')
            
        #if last date is Jan, then chain it to december
        if(un.columns[-1].month == 1):
            print('chaining jan')
            jancol=un.columns[-1]
            prevdec=un.columns[-2]
            for index,value in un.iloc[:,-1].items():
                un.at[index,jancol]=un.loc[index,prevdec]*value/100
        
        un.set_index("ITEM_ID",inplace=True)

        #create a copy of unchained to create the chained indices
        chained = un.copy()

        for col in chained:
            for i, row_value in chained[col].items():
                # print(col,i,row_value,meta.loc[i,'ITEM_START'])
                if(col>=meta.loc[i,'ITEM_START']):
                    if(col==startref):
                        chained.at[i,col]=100
                    # elif(col==meta.loc[i,'ITEM_START']):
                    #     sample.at[i,col]=row_value
                    elif(col<=startref+pd.tseries.offsets.DateOffset(years=1)):
                        chained.at[i,col]=row_value
                    else:
                        if(col.month==1 and col>startref+pd.tseries.offsets.DateOffset(years=1)):
                            chained.at[i,col]=float(row_value)*float(chained.loc[i][datetime(col.year-1,1,1)])/100
                        else:
                            chained.at[i,col]=float(row_value)*float(chained.loc[i][datetime(col.year,1,1)])/100
                            
                elif(col==meta.loc[i,'ITEM_START']-pd.tseries.offsets.DateOffset(months=1)):
                    chained.at[i,col]=100
                    
                else:
                    chained.at[i,col]=None

        

        # Then calculate average prices
        avgprice=chained.copy()

        for col in avgprice:
            for i, row_value in avgprice[col].items():
                if(row_value==None):
                    avgprice.at[i,col]=None
                else:
                    avgprice.at[i,col]=float(row_value)/ \
                    float(chained.loc[i,avgpriceRefMonth])* \
                    float(meta.loc[i,'AVERAGE_PRICE'])
        #rename columns to dates without time formats
        columns = {}
        for col in avgprice.columns:
            try:
                columns[col] = col.date()
            except ValueError:
                pass
        avgprice.rename(columns=columns, inplace=True)

        avgprice.astype(float).round(2).to_csv('avgprice.csv',date_format='%Y-%m-%d',na_rep='')

        # calculate annual growth
        annualgrowth=chained.copy()

        for col in annualgrowth:
            for i, row_value in annualgrowth[col].items():
                if(col<meta.loc[i,'ITEM_START']+pd.tseries.offsets.DateOffset(years=1,months=-1)):
                    annualgrowth.at[i,col]=None
                else:
                    if(col<startref+pd.tseries.offsets.DateOffset(years=1)):
                        annualgrowth.at[i,col]=None
                    else:
                        annualgrowth.at[i,col]=(float(row_value)- \
                        float(chained.loc[i,col-pd.tseries.offsets.DateOffset(years=1)])) * 100 / \
                        float(chained.loc[i,col-pd.tseries.offsets.DateOffset(years=1)])
        #rename columns to dates without time formats
        columns = {}
        for col in annualgrowth.columns:
            try:
                columns[col] = col.date()
            except ValueError:
                pass
        annualgrowth.rename(columns=columns, inplace=True)                
                        
        annualgrowth.astype(float).round(0).astype(int,errors='ignore').to_csv('annualgrowth.csv',date_format='%Y-%m-%d',na_rep='',float_format="%.0f")

        # calculate monthly growth
        monthlygrowth=chained.copy()

        for col in monthlygrowth:
            for i, row_value in monthlygrowth[col].items():
                if(col<meta.loc[i,'ITEM_START']):
                    monthlygrowth.at[i,col]=None
                else:
                    if(col<startref+pd.tseries.offsets.DateOffset(months=1)):
                        monthlygrowth.at[i,col]=None
                    else:
                        monthlygrowth.at[i,col]=(float(row_value)- \
                        float(chained.loc[i,col-pd.tseries.offsets.DateOffset(months=1)])) * 100 / \
                        float(chained.loc[i,col-pd.tseries.offsets.DateOffset(months=1)])

        #rename columns to dates without time formats
        columns = {}
        for col in monthlygrowth.columns:
            try:
                columns[col] = col.date()
            except ValueError:
                pass
        monthlygrowth.rename(columns=columns, inplace=True)

        monthlygrowth.astype(float).round(0).astype(int,errors='ignore').to_csv('monthlygrowth.csv',date_format='%Y-%m-%d',na_rep='',float_format="%.0f")
        
        #Finally save the unchained and chainedvnumbers to csv
        #rename columns to dates without time formats
        columns = {}
        for col in un.columns:
            try:
                columns[col] = col.date()
            except ValueError:
                pass
        un.rename(columns=columns, inplace=True)
        
        #and save it
        un.to_csv('unchained.csv')

        #rename columns to dates without time formats
        columns = {}
        for col in chained.columns:
            try:
                columns[col] = col.date()
            except ValueError:
                pass
        chained.rename(columns=columns, inplace=True)

        chained.astype(float).round(3).to_csv('chained.csv',date_format='%Y-%m-%d',na_rep='')

        #turn it into a excel datadownload file
        with pd.ExcelWriter("datadownload.xlsx", mode="a", if_sheet_exists="replace", date_format="YYYY-MM-DD", datetime_format="YYYY-MM-DD") as writer:
            meta.drop(columns=['AVERAGE_PRICE']).to_excel(writer, sheet_name="metadata")  
            # un.to_excel(writer, sheet_name="unchained")
            chained.astype(float).round(3).to_excel(writer, sheet_name="chained")
            avgprice.astype(float).round(2).fillna('').to_excel(writer, sheet_name="averageprice")
            monthlygrowth.astype(float).round(0).fillna('').to_excel(writer, sheet_name="monthlygrowth")
            annualgrowth.astype(float).round(0).fillna('').to_excel(writer,sheet_name="annualgrowth")

    else:
        print('Nothing to update')  
else:
    print('Automatic updates disabled')
